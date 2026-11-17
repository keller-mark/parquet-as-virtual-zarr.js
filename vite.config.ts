import { defineConfig } from "vite";

// Bundles src/index.ts together with its dependencies into dist/bundled/,
// alongside the unbundled tsc output in dist/ (which keeps dependencies external).
export default defineConfig({
  build: {
    outDir: "dist/bundled",
    emptyOutDir: true,
    sourcemap: true,
    // No minification, so stack traces and debugging remain readable.
    minify: false,
    lib: {
      entry: "src/index.ts",
      formats: ["es"],
      fileName: () => "index.js",
    },
    rollupOptions: {
      external: [
        // zarrita must stay external: we register custom codecs (snappy, brotli)
        // in zarrita's codec registry, which must be the same registry instance
        // that the consumer's zarrita uses when reading from the store.
        // (Consumers need zarrita anyway in order to read from the store.)
        "zarrita",
        // node:zlib is only dynamically imported (for Brotli) inside a try/catch.
        // Keep it external so it still works in Node.js, and fails gracefully in browsers.
        "node:zlib",
      ],
      output: {
        // Produce a single file, rather than splitting dynamic imports
        // (e.g., numcodecs/zstd) into separate chunks.
        inlineDynamicImports: true,
      },
    },
  },
});
