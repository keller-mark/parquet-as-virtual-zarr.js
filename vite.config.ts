import { defineConfig } from "vite";

// Bundles src/index.ts together with its dependencies, alongside the unbundled
// tsc output in dist/ (which keeps dependencies external).
//
// Two variants are built:
// - `vite build` -> dist/bundled/: a single file, with dynamic imports
//   (e.g., numcodecs/zstd and its WASM) inlined.
// - `vite build --mode bundled-dynamic` -> dist/bundled-dynamic/: dynamic
//   imports are emitted as separate chunks, so numcodecs is only loaded
//   when it is actually needed (e.g., when decoding ZSTD-compressed data).
export default defineConfig(({ mode }) => {
  const isDynamic = mode === "bundled-dynamic";
  return {
    build: {
      outDir: isDynamic ? "dist/bundled-dynamic" : "dist/bundled",
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
          // Inline dynamic imports (single file) unless building the dynamic variant.
          inlineDynamicImports: !isDynamic,
        },
      },
    },
  };
});
