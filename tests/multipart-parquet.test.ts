/**
 * Tests for multi-part parquet directory support.
 * Verifies that a multi-part parquet directory (part.0.parquet, part.1.parquet, …)
 * produces the same virtual Zarr store as a single-file parquet.
 */

import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import { describe, test, expect, beforeAll } from "vitest";
// @ts-ignore - hyparquet is a JS package
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import FileSystemStore from "@zarrita/storage/fs";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";
import { open, get, root } from "zarrita";
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const MULTIPART_DIR = resolve(__dirname, "../fixtures/output/obs_multipart");
const SINGLE_PARQUET_PATH = resolve(__dirname, "../fixtures/output/obs.parquet");

// ── fixtures ───────────────────────────────────────────────────────────────

let multiStore: ParquetAsAnnDataFrameStore;
let singleStore: ParquetAsAnnDataFrameStore;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquetMeta: any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let asyncBuf: any;

beforeAll(async () => {
  multiStore = ParquetAsAnnDataFrameStore.fromStore(
    new FileSystemStore(MULTIPART_DIR)
  );
  singleStore = ParquetAsAnnDataFrameStore.fromStore(
    new FileSystemStore(SINGLE_PARQUET_PATH)
  );
  asyncBuf = await asyncBufferFromFile(SINGLE_PARQUET_PATH);
  parquetMeta = await parquetMetadataAsync(asyncBuf);
});

// ── metadata tests ─────────────────────────────────────────────────────────

describe("multi-part root /zarr.json", () => {
  test("matches single-file store", async () => {
    const grpMulti = await open(root(multiStore), { kind: "group" });
    const grpSingle = await open(root(singleStore), { kind: "group" });
    expect(grpMulti.kind).toBe(grpSingle.kind);
    expect(grpMulti.attrs).toEqual(grpSingle.attrs);
  });
});

describe("multi-part column metadata", () => {
  test("numeric column nullable group zarr.json matches single-file", async () => {
    const grpMulti = await open(root(multiStore).resolve("n_counts"), { kind: "group" });
    const grpSingle = await open(root(singleStore).resolve("n_counts"), { kind: "group" });
    expect(grpMulti.kind).toBe(grpSingle.kind);
    expect(grpMulti.attrs).toEqual(grpSingle.attrs);
    const arrMulti = await open(root(multiStore).resolve("n_counts/values"), { kind: "array" });
    const arrSingle = await open(root(singleStore).resolve("n_counts/values"), { kind: "array" });
    expect(arrMulti.dtype).toBe(arrSingle.dtype);
    expect(arrMulti.shape).toEqual(arrSingle.shape);
    expect(arrMulti.attrs).toEqual(arrSingle.attrs);
  });

  test("string column zarr.json matches single-file", async () => {
    const arrMulti = await open(root(multiStore).resolve("obs_id"), { kind: "array" });
    const arrSingle = await open(root(singleStore).resolve("obs_id"), { kind: "array" });
    expect(arrMulti.dtype).toBe(arrSingle.dtype);
    expect(arrMulti.shape).toEqual(arrSingle.shape);
    expect(arrMulti.attrs).toEqual(arrSingle.attrs);
  });

  test("categorical group zarr.json matches single-file", async () => {
    const grpMulti = await open(root(multiStore).resolve("cell_type"), { kind: "group" });
    const grpSingle = await open(root(singleStore).resolve("cell_type"), { kind: "group" });
    expect(grpMulti.kind).toBe(grpSingle.kind);
    expect(grpMulti.attrs).toEqual(grpSingle.attrs);
  });

  test("codes zarr.json matches single-file", async () => {
    const arrMulti = await open(root(multiStore).resolve("cell_type/codes"), { kind: "array" });
    const arrSingle = await open(root(singleStore).resolve("cell_type/codes"), { kind: "array" });
    expect(arrMulti.dtype).toBe(arrSingle.dtype);
    expect(arrMulti.shape).toEqual(arrSingle.shape);
    expect(arrMulti.attrs).toEqual(arrSingle.attrs);
  });

  test("categories zarr.json matches single-file", async () => {
    const arrMulti = await open(root(multiStore).resolve("cell_type/categories"), { kind: "array" });
    const arrSingle = await open(root(singleStore).resolve("cell_type/categories"), { kind: "array" });
    expect(arrMulti.dtype).toBe(arrSingle.dtype);
    expect(arrMulti.shape).toEqual(arrSingle.shape);
    expect(arrMulti.attrs).toEqual(arrSingle.attrs);
  });
});

// ── data tests ─────────────────────────────────────────────────────────────

describe("multi-part numeric data", () => {
  test("n_counts values match single-file parquet", async () => {
    const arr = await open(root(multiStore).resolve("n_counts/values"), { kind: "array" });
    const chunk = await get(arr);
    const multiValues = Array.from(chunk.data as Float32Array);
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["n_counts"],
    })) as Record<string, number>[];
    expect(multiValues).toEqual(rows.map((r) => r.n_counts));
  });

  test("n_genes values match single-file parquet", async () => {
    const arr = await open(root(multiStore).resolve("n_genes/values"), { kind: "array" });
    const chunk = await get(arr);
    const multiValues = Array.from(chunk.data as Int32Array);
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["n_genes"],
    })) as Record<string, number>[];
    expect(multiValues).toEqual(rows.map((r) => r.n_genes));
  });
});

describe("multi-part string data", () => {
  test("obs_id values match single-file parquet", async () => {
    const arr = await open(root(multiStore).resolve("obs_id"), { kind: "array" });
    const chunk = await get(arr);
    const multiValues = chunk.data as string[];
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["obs_id"],
    })) as Record<string, string>[];
    expect(multiValues).toEqual(rows.map((r) => r.obs_id));
  });
});

describe("multi-part categorical data", () => {
  test("cell_type categories match single-file", async () => {
    const catsArrMulti = await open(root(multiStore).resolve("cell_type/categories"), { kind: "array" });
    const catsArrSingle = await open(root(singleStore).resolve("cell_type/categories"), { kind: "array" });
    const multiCategories = (await get(catsArrMulti)).data as string[];
    const singleCategories = (await get(catsArrSingle)).data as string[];
    expect(multiCategories).toEqual(singleCategories);
  });

  test("cell_type codes round-trip to original values", async () => {
    const catsArr = await open(root(multiStore).resolve("cell_type/categories"), { kind: "array" });
    const categories = (await get(catsArr)).data as string[];

    const codesArr = await open(root(multiStore).resolve("cell_type/codes"), { kind: "array" });
    const codes = (await get(codesArr)).data as Int8Array;
    const decoded = Array.from(codes, (code) => categories[code]);

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["cell_type"],
    })) as Record<string, string>[];
    expect(decoded).toEqual(rows.map((r) => r.cell_type));
  });

  test("leiden categories and codes match single-file", async () => {
    const catsArrMulti = await open(root(multiStore).resolve("leiden/categories"), { kind: "array" });
    const catsArrSingle = await open(root(singleStore).resolve("leiden/categories"), { kind: "array" });
    const multiCategories = (await get(catsArrMulti)).data as string[];
    const singleCategories = (await get(catsArrSingle)).data as string[];
    expect(multiCategories).toEqual(singleCategories);
  });
});

// ── partial read tests for multi-part ──────────────────────────────────────

/**
 * In-memory store for multi-part parquet directory.
 * Serves keys like "/part.0.parquet", "/part.1.parquet", etc.
 * "/" returns undefined (not a single-file store).
 */
class MultiPartStoreSpy implements AsyncReadable {
  readonly #parts: Map<string, ArrayBuffer>;
  readonly fetchCalls: Array<{ key: string; offset: number; length: number }> = [];
  getCalls = 0;

  constructor(dirPath: string) {
    this.#parts = new Map();
    const files = readdirSync(dirPath).filter((f) => f.endsWith(".parquet")).sort();
    for (const file of files) {
      const raw = readFileSync(resolve(dirPath, file));
      const ab = raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength);
      this.#parts.set(`/${file}`, ab as ArrayBuffer);
    }
  }

  async get(_key: AbsolutePath): Promise<Uint8Array | undefined> {
    this.getCalls++;
    return undefined;
  }

  async getRange(
    key: AbsolutePath,
    range: RangeQuery,
  ): Promise<Uint8Array | undefined> {
    const buf = this.#parts.get(key);
    if (!buf) return undefined;

    let offset: number;
    let length: number;
    if ("suffixLength" in range) {
      offset = buf.byteLength - range.suffixLength;
      length = range.suffixLength;
    } else {
      offset = range.offset;
      length = range.length;
    }
    this.fetchCalls.push({ key, offset, length });
    return new Uint8Array(buf, offset, length);
  }
}

describe("multi-part partial reads", () => {
  test("get() is never called — init uses only getRange", async () => {
    const spy = new MultiPartStoreSpy(MULTIPART_DIR);
    const s = ParquetAsAnnDataFrameStore.fromStore(spy);
    await open(root(s), { kind: "group" });
    await s.get("/n_counts/values/c/0");
    expect(spy.getCalls).toBe(0);
  });

  test("each part file footer is read during init", async () => {
    const spy = new MultiPartStoreSpy(MULTIPART_DIR);
    const s = ParquetAsAnnDataFrameStore.fromStore(spy);
    await open(root(s), { kind: "group" });

    // Should have read footers from all 4 parts + attempted part.4.parquet (returns undefined)
    const partKeys = new Set(spy.fetchCalls.map((c) => c.key));
    expect(partKeys.has("/part.0.parquet")).toBe(true);
    expect(partKeys.has("/part.1.parquet")).toBe(true);
    expect(partKeys.has("/part.2.parquet")).toBe(true);
    expect(partKeys.has("/part.3.parquet")).toBe(true);
  });

  test("chunk reads target the correct part file", async () => {
    const spy = new MultiPartStoreSpy(MULTIPART_DIR);
    const s = ParquetAsAnnDataFrameStore.fromStore(spy);
    await open(root(s), { kind: "group" });

    // Read chunk from row group 0 → should read from part.0.parquet
    const before0 = spy.fetchCalls.length;
    await s.get("/n_counts/values/c/0");
    const call0 = spy.fetchCalls.slice(before0);
    expect(call0.length).toBe(1);
    expect(call0[0].key).toBe("/part.0.parquet");

    // Read chunk from row group 2 → should read from part.2.parquet
    const before2 = spy.fetchCalls.length;
    await s.get("/n_counts/values/c/2");
    const call2 = spy.fetchCalls.slice(before2);
    expect(call2.length).toBe(1);
    expect(call2[0].key).toBe("/part.2.parquet");
  });
});
