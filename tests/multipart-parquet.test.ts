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
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const MULTIPART_DIR = resolve(__dirname, "../fixtures/output/obs_multipart");
const SINGLE_PARQUET_PATH = resolve(__dirname, "../fixtures/output/obs.parquet");

// ── helpers ────────────────────────────────────────────────────────────────

function decodeVlenUtf8(bytes: Uint8Array): string[] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(0, true);
  const decoder = new TextDecoder();
  const strings: string[] = [];
  let pos = 4;
  for (let i = 0; i < count; i++) {
    const len = view.getUint32(pos, true);
    pos += 4;
    strings.push(
      decoder.decode(new Uint8Array(bytes.buffer, bytes.byteOffset + pos, len))
    );
    pos += len;
  }
  return strings;
}

async function getJson(
  store: ParquetAsAnnDataFrameStore,
  key: `/${string}`
): Promise<Record<string, unknown>> {
  const bytes = await store.get(key);
  if (!bytes) throw new Error(`store returned undefined for ${key}`);
  return JSON.parse(new TextDecoder().decode(bytes));
}

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
    const multi = await getJson(multiStore, "/zarr.json");
    const single = await getJson(singleStore, "/zarr.json");
    expect(multi).toEqual(single);
  });
});

describe("multi-part column metadata", () => {
  test("numeric column zarr.json matches single-file", async () => {
    const multi = await getJson(multiStore, "/n_counts/zarr.json");
    const single = await getJson(singleStore, "/n_counts/zarr.json");
    expect(multi).toEqual(single);
  });

  test("string column zarr.json matches single-file", async () => {
    const multi = await getJson(multiStore, "/obs_id/zarr.json");
    const single = await getJson(singleStore, "/obs_id/zarr.json");
    expect(multi).toEqual(single);
  });

  test("categorical group zarr.json matches single-file", async () => {
    const multi = await getJson(multiStore, "/cell_type/zarr.json");
    const single = await getJson(singleStore, "/cell_type/zarr.json");
    expect(multi).toEqual(single);
  });

  test("codes zarr.json matches single-file", async () => {
    const multi = await getJson(multiStore, "/cell_type/codes/zarr.json");
    const single = await getJson(singleStore, "/cell_type/codes/zarr.json");
    expect(multi).toEqual(single);
  });

  test("categories zarr.json matches single-file", async () => {
    const multi = await getJson(multiStore, "/cell_type/categories/zarr.json");
    const single = await getJson(singleStore, "/cell_type/categories/zarr.json");
    expect(multi).toEqual(single);
  });
});

// ── data tests ─────────────────────────────────────────────────────────────

describe("multi-part numeric data", () => {
  test("n_counts values match single-file parquet", async () => {
    const numRgs = parquetMeta.row_groups.length;
    const multiValues: number[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await multiStore.get(`/n_counts/c/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const floats = new Float32Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 4);
      multiValues.push(...Array.from(floats));
    }
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["n_counts"],
    })) as Record<string, number>[];
    expect(multiValues).toEqual(rows.map((r) => r.n_counts));
  });

  test("n_genes values match single-file parquet", async () => {
    const numRgs = parquetMeta.row_groups.length;
    const multiValues: number[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await multiStore.get(`/n_genes/c/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const ints = new Int32Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 4);
      multiValues.push(...Array.from(ints));
    }
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
    const numRgs = parquetMeta.row_groups.length;
    const multiValues: string[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await multiStore.get(`/obs_id/c/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      multiValues.push(...decodeVlenUtf8(bytes!));
    }
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
    const multiBytes = await multiStore.get("/cell_type/categories/c/0");
    const singleBytes = await singleStore.get("/cell_type/categories/c/0");
    expect(multiBytes).toBeDefined();
    expect(singleBytes).toBeDefined();
    expect(decodeVlenUtf8(multiBytes!)).toEqual(decodeVlenUtf8(singleBytes!));
  });

  test("cell_type codes round-trip to original values", async () => {
    const catBytes = await multiStore.get("/cell_type/categories/c/0");
    const categories = decodeVlenUtf8(catBytes!);

    const numRgs = parquetMeta.row_groups.length;
    const decoded: string[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await multiStore.get(`/cell_type/codes/c/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const codes = new Int8Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength);
      for (const code of codes) decoded.push(categories[code]);
    }

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["cell_type"],
    })) as Record<string, string>[];
    expect(decoded).toEqual(rows.map((r) => r.cell_type));
  });

  test("leiden categories and codes match single-file", async () => {
    const multiCatBytes = await multiStore.get("/leiden/categories/c/0");
    const singleCatBytes = await singleStore.get("/leiden/categories/c/0");
    expect(decodeVlenUtf8(multiCatBytes!)).toEqual(decodeVlenUtf8(singleCatBytes!));
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
    await s.get("/zarr.json");
    await s.get("/n_counts/c/0");
    expect(spy.getCalls).toBe(0);
  });

  test("each part file footer is read during init", async () => {
    const spy = new MultiPartStoreSpy(MULTIPART_DIR);
    const s = ParquetAsAnnDataFrameStore.fromStore(spy);
    await s.get("/zarr.json");

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
    await s.get("/zarr.json");

    // Read chunk from row group 0 → should read from part.0.parquet
    const before0 = spy.fetchCalls.length;
    await s.get("/n_counts/c/0");
    const call0 = spy.fetchCalls.slice(before0);
    expect(call0.length).toBe(1);
    expect(call0[0].key).toBe("/part.0.parquet");

    // Read chunk from row group 2 → should read from part.2.parquet
    const before2 = spy.fetchCalls.length;
    await s.get("/n_counts/c/2");
    const call2 = spy.fetchCalls.slice(before2);
    expect(call2.length).toBe(1);
    expect(call2[0].key).toBe("/part.2.parquet");
  });
});
