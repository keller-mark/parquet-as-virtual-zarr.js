/**
 * Tests for the zero-copy pass-through path.
 *
 * When a parquet column is REQUIRED + PLAIN-encoded + numeric:
 * - SNAPPY columns: raw compressed page data is returned and zarr metadata
 *   includes the snappy codec so zarrita can decompress.
 * - UNCOMPRESSED columns: raw bytes are returned directly.
 *
 * These tests use special fixtures (obs_plain_snappy.parquet, obs_plain_none.parquet)
 * that have REQUIRED numeric columns with PLAIN encoding.
 */

import { readFileSync, readFile as readFileCb } from "node:fs";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import { describe, test, expect, beforeAll } from "vitest";
// @ts-ignore - hyparquet is a JS package
import { parquetMetadata, parquetRead } from "hyparquet";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";
// Ensure snappy codec is registered
import "../src/snappy-codec.js";
import { snappyDecode } from "../src/vendored/snappy.js";

const readFile = promisify(readFileCb);
const __dirname = dirname(fileURLToPath(import.meta.url));
const PLAIN_SNAPPY_PATH = resolve(__dirname, "../fixtures/output/obs_plain_snappy.parquet");
const PLAIN_NONE_PATH = resolve(__dirname, "../fixtures/output/obs_plain_none.parquet");
const ZARR_OBS_PLAIN = resolve(__dirname, "../fixtures/output/adata_plain.zarr/obs");

// ── helpers ────────────────────────────────────────────────────────────────

/** In-memory AsyncReadable wrapper. */
class MemStore implements AsyncReadable {
  readonly #buf: ArrayBuffer;
  readonly fileSize: number;

  constructor(fileBuffer: ArrayBuffer) {
    this.#buf = fileBuffer;
    this.fileSize = fileBuffer.byteLength;
  }

  async get(_key: AbsolutePath): Promise<Uint8Array | undefined> {
    return new Uint8Array(this.#buf);
  }

  async getRange(
    _key: AbsolutePath,
    range: RangeQuery,
  ): Promise<Uint8Array | undefined> {
    let offset: number;
    let length: number;
    if ("suffixLength" in range) {
      offset = this.fileSize - range.suffixLength;
      length = range.suffixLength;
    } else {
      offset = range.offset;
      length = range.length;
    }
    return new Uint8Array(this.#buf, offset, length);
  }
}

/** Create a typed array from a Uint8Array, handling alignment. */
function toFloat32Array(bytes: Uint8Array): Float32Array {
  if (bytes.byteOffset % 4 === 0) {
    return new Float32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 4);
  }
  const aligned = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(aligned).set(bytes);
  return new Float32Array(aligned);
}

function toInt32Array(bytes: Uint8Array): Int32Array {
  if (bytes.byteOffset % 4 === 0) {
    return new Int32Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 4);
  }
  const aligned = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(aligned).set(bytes);
  return new Int32Array(aligned);
}

async function getJson(
  store: ParquetAsAnnDataFrameStore,
  key: `/${string}`,
): Promise<Record<string, unknown>> {
  const bytes = await store.get(key);
  if (!bytes) throw new Error(`store returned undefined for ${key}`);
  return JSON.parse(new TextDecoder().decode(bytes));
}

/** Read and parse a zarr.json file from the ground-truth zarr store. */
async function zarrMeta(subpath: string): Promise<Record<string, unknown>> {
  const text = await readFile(resolve(ZARR_OBS_PLAIN, subpath), "utf-8");
  return JSON.parse(text);
}

/** Read reference parquet data via hyparquet parquetRead (for comparison). */
async function readParquetColumn(
  filePath: string,
  colName: string,
): Promise<Float32Array | Int32Array> {
  const buf = readFileSync(filePath);
  const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  const metadata = parquetMetadata(ab);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const chunks: any[] = [];
  await parquetRead({
    file: { byteLength: ab.byteLength, slice: (s: number, e: number) => ab.slice(s, e) },
    metadata,
    columns: [colName],
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    onChunk(chunk: any) {
      chunks.push(chunk.columnData);
    },
  });
  if (chunks.length === 1) return chunks[0];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const Ctor = chunks[0].constructor as { new (len: number): any };
  const totalLen = chunks.reduce((s: number, c: { length: number }) => s + c.length, 0);
  const merged = new Ctor(totalLen);
  let offset = 0;
  for (const c of chunks) {
    merged.set(c, offset);
    offset += c.length;
  }
  return merged;
}

// ── SNAPPY fixtures ─────────────────────────────────────────────────────

describe("zero-copy SNAPPY pass-through", () => {
  let store: ParquetAsAnnDataFrameStore;

  beforeAll(() => {
    const buf = readFileSync(PLAIN_SNAPPY_PATH);
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    store = ParquetAsAnnDataFrameStore.fromStore(new MemStore(ab));
  });

  test("numeric column metadata includes snappy codec", async () => {
    const meta = await getJson(store, "/n_counts/zarr.json");
    expect(meta.node_type).toBe("array");
    expect(meta.data_type).toBe("float32");
    const codecs = meta.codecs as { name: string }[];
    expect(codecs).toHaveLength(2);
    expect(codecs[0].name).toBe("bytes");
    expect(codecs[1].name).toBe("snappy");
  });

  test("n_genes metadata includes snappy codec", async () => {
    const meta = await getJson(store, "/n_genes/zarr.json");
    expect(meta.data_type).toBe("int32");
    const codecs = meta.codecs as { name: string }[];
    expect(codecs).toHaveLength(2);
    expect(codecs[1].name).toBe("snappy");
  });

  test("obs_id (string column) does NOT include snappy codec", async () => {
    const meta = await getJson(store, "/obs_id/zarr.json");
    expect(meta.data_type).toBe("string");
    const codecs = meta.codecs as { name: string }[];
    expect(codecs).toHaveLength(1);
    expect(codecs[0].name).toBe("vlen-utf8");
  });

  test("n_counts chunk bytes are snappy-compressed and decode correctly", async () => {
    const chunkBytes = await store.get("/n_counts/c/0" as AbsolutePath);
    expect(chunkBytes).toBeDefined();

    // The returned bytes should be snappy-compressed (not raw float32)
    // Decompress and verify against reference
    const decompressed = snappyDecode(chunkBytes!);
    const values = toFloat32Array(decompressed);

    // Compare against hyparquet's decoded values
    const reference = await readParquetColumn(PLAIN_SNAPPY_PATH, "n_counts") as Float32Array;
    // First chunk = first row_group_size values
    const chunkSize = values.length;
    for (let i = 0; i < chunkSize; i++) {
      expect(values[i]).toBeCloseTo(reference[i], 5);
    }
  });

  test("n_genes chunk bytes are snappy-compressed and decode correctly", async () => {
    const chunkBytes = await store.get("/n_genes/c/0" as AbsolutePath);
    expect(chunkBytes).toBeDefined();

    const decompressed = snappyDecode(chunkBytes!);
    const values = toInt32Array(decompressed);

    const reference = await readParquetColumn(PLAIN_SNAPPY_PATH, "n_genes") as Int32Array;
    const chunkSize = values.length;
    for (let i = 0; i < chunkSize; i++) {
      expect(values[i]).toBe(reference[i]);
    }
  });

  test("all chunks decode to match full reference data", async () => {
    const reference = await readParquetColumn(PLAIN_SNAPPY_PATH, "n_counts") as Float32Array;
    const allValues: number[] = [];

    for (let rg = 0; rg < 4; rg++) {
      const chunkBytes = await store.get(`/n_counts/c/${rg}` as AbsolutePath);
      expect(chunkBytes).toBeDefined();
      const decompressed = snappyDecode(chunkBytes!);
      const values = toFloat32Array(decompressed);
      allValues.push(...values);
    }

    expect(allValues.length).toBe(reference.length);
    for (let i = 0; i < reference.length; i++) {
      expect(allValues[i]).toBeCloseTo(reference[i], 5);
    }
  });

  test("consolidated metadata includes snappy codec for numeric columns", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const meta = (consolidated.consolidated_metadata as any).metadata;
    const nCountsMeta = meta["n_counts"] as Record<string, unknown>;
    const codecs = nCountsMeta.codecs as { name: string }[];
    expect(codecs.some((c) => c.name === "snappy")).toBe(true);
  });
});

// ── UNCOMPRESSED fixtures ───────────────────────────────────────────────

describe("zero-copy UNCOMPRESSED pass-through", () => {
  let store: ParquetAsAnnDataFrameStore;

  beforeAll(() => {
    const buf = readFileSync(PLAIN_NONE_PATH);
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    store = ParquetAsAnnDataFrameStore.fromStore(new MemStore(ab));
  });

  test("numeric column metadata does NOT include snappy codec", async () => {
    const meta = await getJson(store, "/n_counts/zarr.json");
    const codecs = meta.codecs as { name: string }[];
    expect(codecs).toHaveLength(1);
    expect(codecs[0].name).toBe("bytes");
  });

  test("n_counts chunk bytes are raw float32 values", async () => {
    const chunkBytes = await store.get("/n_counts/c/0" as AbsolutePath);
    expect(chunkBytes).toBeDefined();

    // For uncompressed PLAIN, the raw bytes ARE the float32 array
    const values = toFloat32Array(chunkBytes!);

    const reference = await readParquetColumn(PLAIN_NONE_PATH, "n_counts") as Float32Array;
    const chunkSize = values.length;
    for (let i = 0; i < chunkSize; i++) {
      expect(values[i]).toBeCloseTo(reference[i], 5);
    }
  });

  test("n_genes chunk bytes are raw int32 values", async () => {
    const chunkBytes = await store.get("/n_genes/c/0" as AbsolutePath);
    expect(chunkBytes).toBeDefined();

    const values = toInt32Array(chunkBytes!);

    const reference = await readParquetColumn(PLAIN_NONE_PATH, "n_genes") as Int32Array;
    const chunkSize = values.length;
    for (let i = 0; i < chunkSize; i++) {
      expect(values[i]).toBe(reference[i]);
    }
  });

  test("all chunks decode to match full reference data", async () => {
    const reference = await readParquetColumn(PLAIN_NONE_PATH, "n_counts") as Float32Array;
    const allValues: number[] = [];

    for (let rg = 0; rg < 4; rg++) {
      const chunkBytes = await store.get(`/n_counts/c/${rg}` as AbsolutePath);
      expect(chunkBytes).toBeDefined();
      const values = toFloat32Array(chunkBytes!);
      allValues.push(...values);
    }

    expect(allValues.length).toBe(reference.length);
    for (let i = 0; i < reference.length; i++) {
      expect(allValues[i]).toBeCloseTo(reference[i], 5);
    }
  });
});

// ── Zarr ground-truth comparison ────────────────────────────────────────

describe("zero-copy data matches zarr ground truth", () => {
  let snappyStore: ParquetAsAnnDataFrameStore;
  let noneStore: ParquetAsAnnDataFrameStore;

  beforeAll(() => {
    const snappyBuf = readFileSync(PLAIN_SNAPPY_PATH);
    const snappyAb = snappyBuf.buffer.slice(snappyBuf.byteOffset, snappyBuf.byteOffset + snappyBuf.byteLength);
    snappyStore = ParquetAsAnnDataFrameStore.fromStore(new MemStore(snappyAb));

    const noneBuf = readFileSync(PLAIN_NONE_PATH);
    const noneAb = noneBuf.buffer.slice(noneBuf.byteOffset, noneBuf.byteOffset + noneBuf.byteLength);
    noneStore = ParquetAsAnnDataFrameStore.fromStore(new MemStore(noneAb));
  });

  test("root metadata matches zarr", async () => {
    const virtual = await getJson(snappyStore, "/zarr.json");
    const actual = await zarrMeta("zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
    expect(vAttrs["encoding-version"]).toBe(aAttrs["encoding-version"]);
    expect(vAttrs["_index"]).toBe(aAttrs["_index"]);
  });

  test("n_counts metadata shape and dtype match zarr (ignoring codecs)", async () => {
    const virtual = await getJson(snappyStore, "/n_counts/zarr.json");
    const actual = await zarrMeta("n_counts/zarr.json");
    expect(virtual.data_type).toBe(actual.data_type);
    expect(virtual.shape).toEqual(actual.shape);
    expect(virtual.fill_value).toBe(actual.fill_value);
  });
});
