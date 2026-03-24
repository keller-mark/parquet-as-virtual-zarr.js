import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import { describe, test, expect, beforeAll } from "vitest";
// @ts-ignore - hyparquet is a JS package
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import FileSystemStore from "@zarrita/storage/fs";
import { ParquetAsAnnDataFrameZarr } from "../src/parquet-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PARQUET_PATH = resolve(__dirname, "../fixtures/output/obs.parquet");
const ZARR_OBS = resolve(__dirname, "../fixtures/output/adata.zarr/obs");

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
      decoder.decode(
        new Uint8Array(bytes.buffer, bytes.byteOffset + pos, len)
      )
    );
    pos += len;
  }
  return strings;
}

async function zarrAttrs(subpath: string): Promise<Record<string, unknown>> {
  const text = await readFile(resolve(ZARR_OBS, subpath), "utf-8");
  return JSON.parse(text);
}

async function getJson(
  store: ParquetAsAnnDataFrameZarr,
  key: `/${string}`
): Promise<Record<string, unknown>> {
  const bytes = await store.get(key);
  if (!bytes) throw new Error(`store returned undefined for ${key}`);
  return JSON.parse(new TextDecoder().decode(bytes));
}

// ── fixtures ───────────────────────────────────────────────────────────────

let store: ParquetAsAnnDataFrameZarr;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquetMeta: any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let asyncBuf: any;

beforeAll(async () => {
  store = ParquetAsAnnDataFrameZarr.fromStore(new FileSystemStore(PARQUET_PATH));
  asyncBuf = await asyncBufferFromFile(PARQUET_PATH);
  parquetMeta = await parquetMetadataAsync(asyncBuf);
});

// ── metadata tests ─────────────────────────────────────────────────────────

describe("root /.zattrs", () => {
  test("encoding-type and encoding-version match zarr", async () => {
    const virtual = await getJson(store, "/.zattrs");
    const actual = await zarrAttrs(".zattrs");
    expect(virtual["encoding-type"]).toBe(actual["encoding-type"]);
    expect(virtual["encoding-version"]).toBe(actual["encoding-version"]);
  });

  test("column-order matches zarr", async () => {
    const virtual = await getJson(store, "/.zattrs");
    const actual = await zarrAttrs(".zattrs");
    expect(virtual["column-order"]).toEqual(actual["column-order"]);
  });

  test("_index matches zarr", async () => {
    const virtual = await getJson(store, "/.zattrs");
    const actual = await zarrAttrs(".zattrs");
    expect(virtual["_index"]).toBe(actual["_index"]);
  });
});

describe("/.zgroup", () => {
  test("zarr_format is 2", async () => {
    const virtual = await getJson(store, "/.zgroup");
    expect(virtual.zarr_format).toBe(2);
  });
});

describe("numeric column n_counts", () => {
  test(".zattrs encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/n_counts/.zattrs");
    const actual = await zarrAttrs("n_counts/.zattrs");
    expect(virtual["encoding-type"]).toBe(actual["encoding-type"]);
  });

  test(".zarray dtype matches zarr", async () => {
    const virtual = await getJson(store, "/n_counts/.zarray");
    const actual = await zarrAttrs("n_counts/.zarray");
    expect(virtual.dtype).toBe(actual.dtype);
  });

  test(".zarray zarr_format is 2", async () => {
    const virtual = await getJson(store, "/n_counts/.zarray");
    expect(virtual.zarr_format).toBe(2);
  });

  test(".zarray shape covers all rows", async () => {
    const virtual = await getJson(store, "/n_counts/.zarray");
    const numRows = parquetMeta.row_groups.reduce(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (s: number, rg: any) => s + Number(rg.num_rows),
      0
    );
    const shape = virtual.shape as number[];
    expect(shape[0]).toBe(numRows);
  });
});

describe("numeric column n_genes", () => {
  test(".zattrs encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/n_genes/.zattrs");
    const actual = await zarrAttrs("n_genes/.zattrs");
    expect(virtual["encoding-type"]).toBe(actual["encoding-type"]);
  });

  test(".zarray dtype matches zarr", async () => {
    const virtual = await getJson(store, "/n_genes/.zarray");
    const actual = await zarrAttrs("n_genes/.zarray");
    expect(virtual.dtype).toBe(actual.dtype);
  });
});

describe("index column obs_id", () => {
  test(".zattrs encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/obs_id/.zattrs");
    const actual = await zarrAttrs("obs_id/.zattrs");
    expect(virtual["encoding-type"]).toBe(actual["encoding-type"]);
  });

  test(".zarray dtype matches zarr", async () => {
    const virtual = await getJson(store, "/obs_id/.zarray");
    const actual = await zarrAttrs("obs_id/.zarray");
    expect(virtual.dtype).toBe(actual.dtype);
  });
});

describe("categorical column cell_type", () => {
  test(".zattrs encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/.zattrs");
    const actual = await zarrAttrs("cell_type/.zattrs");
    expect(virtual["encoding-type"]).toBe(actual["encoding-type"]);
    expect(virtual["encoding-version"]).toBe(actual["encoding-version"]);
  });

  test(".zattrs ordered matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/.zattrs");
    const actual = await zarrAttrs("cell_type/.zattrs");
    expect(virtual.ordered).toBe(actual.ordered);
  });

  test(".zgroup zarr_format is 2", async () => {
    const virtual = await getJson(store, "/cell_type/.zgroup");
    expect(virtual.zarr_format).toBe(2);
  });

  test("codes .zarray dtype matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/codes/.zarray");
    const actual = await zarrAttrs("cell_type/codes/.zarray");
    expect(virtual.dtype).toBe(actual.dtype);
  });

  test("codes .zarray zarr_format is 2", async () => {
    const virtual = await getJson(store, "/cell_type/codes/.zarray");
    expect(virtual.zarr_format).toBe(2);
  });

  test("categories .zarray dtype matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/categories/.zarray");
    const actual = await zarrAttrs("cell_type/categories/.zarray");
    expect(virtual.dtype).toBe(actual.dtype);
  });

  test("categories .zarray shape matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/categories/.zarray");
    const actual = await zarrAttrs("cell_type/categories/.zarray");
    expect(virtual.shape).toEqual(actual.shape);
  });
});

describe("categorical column leiden", () => {
  test(".zattrs encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/leiden/.zattrs");
    const actual = await zarrAttrs("leiden/.zattrs");
    expect(virtual["encoding-type"]).toBe(actual["encoding-type"]);
  });

  test("codes .zarray dtype matches zarr", async () => {
    const virtual = await getJson(store, "/leiden/codes/.zarray");
    const actual = await zarrAttrs("leiden/codes/.zarray");
    expect(virtual.dtype).toBe(actual.dtype);
  });

  test("categories .zarray shape matches zarr", async () => {
    const virtual = await getJson(store, "/leiden/categories/.zarray");
    const actual = await zarrAttrs("leiden/categories/.zarray");
    expect(virtual.shape).toEqual(actual.shape);
  });
});

// ── array data tests ────────────────────────────────────────────────────────

describe("n_counts array data", () => {
  test("all row groups values match parquet", async () => {
    const numRgs = parquetMeta.row_groups.length;
    // Collect all values from virtual store
    const virtualValues: number[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/n_counts/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const floats = new Float32Array(
        bytes!.buffer,
        bytes!.byteOffset,
        bytes!.byteLength / 4
      );
      virtualValues.push(...Array.from(floats));
    }
    // Read full column from parquet
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["n_counts"],
    })) as Record<string, number>[];
    const expected = rows.map((r) => r.n_counts);
    expect(virtualValues).toEqual(expected);
  });
});

describe("n_genes array data", () => {
  test("all row groups values match parquet", async () => {
    const numRgs = parquetMeta.row_groups.length;
    const virtualValues: number[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/n_genes/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const ints = new Int32Array(
        bytes!.buffer,
        bytes!.byteOffset,
        bytes!.byteLength / 4
      );
      virtualValues.push(...Array.from(ints));
    }
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["n_genes"],
    })) as Record<string, number>[];
    const expected = rows.map((r) => r.n_genes);
    expect(virtualValues).toEqual(expected);
  });
});

describe("obs_id string array data", () => {
  test("all row groups values match parquet", async () => {
    const numRgs = parquetMeta.row_groups.length;
    const virtualValues: string[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/obs_id/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      virtualValues.push(...decodeVlenUtf8(bytes!));
    }
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["obs_id"],
    })) as Record<string, string>[];
    const expected = rows.map((r) => r.obs_id);
    expect(virtualValues).toEqual(expected);
  });
});

describe("cell_type categorical data", () => {
  test("categories are sorted unique values from parquet", async () => {
    const bytes = await store.get("/cell_type/categories/0");
    expect(bytes).toBeDefined();
    const virtualCategories = decodeVlenUtf8(bytes!);

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["cell_type"],
    })) as Record<string, string>[];
    const expectedCategories = [...new Set(rows.map((r) => r.cell_type))].sort();
    expect(virtualCategories).toEqual(expectedCategories);
  });

  test("codes round-trip back to original string values", async () => {
    const catBytes = await store.get("/cell_type/categories/0");
    expect(catBytes).toBeDefined();
    const categories = decodeVlenUtf8(catBytes!);

    const numRgs = parquetMeta.row_groups.length;
    const decoded: string[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/cell_type/codes/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const codes = new Int8Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength);
      for (const code of codes) {
        decoded.push(categories[code]);
      }
    }

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["cell_type"],
    })) as Record<string, string>[];
    const expected = rows.map((r) => r.cell_type);
    expect(decoded).toEqual(expected);
  });
});

describe("leiden categorical data", () => {
  test("categories are sorted unique values from parquet", async () => {
    const bytes = await store.get("/leiden/categories/0");
    expect(bytes).toBeDefined();
    const virtualCategories = decodeVlenUtf8(bytes!);

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["leiden"],
    })) as Record<string, string>[];
    const expectedCategories = [...new Set(rows.map((r) => r.leiden))].sort();
    expect(virtualCategories).toEqual(expectedCategories);
  });

  test("codes round-trip back to original string values", async () => {
    const catBytes = await store.get("/leiden/categories/0");
    expect(catBytes).toBeDefined();
    const categories = decodeVlenUtf8(catBytes!);

    const numRgs = parquetMeta.row_groups.length;
    const decoded: string[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/leiden/codes/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const codes = new Int8Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength);
      for (const code of codes) {
        decoded.push(categories[code]);
      }
    }

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["leiden"],
    })) as Record<string, string>[];
    const expected = rows.map((r) => r.leiden);
    expect(decoded).toEqual(expected);
  });
});

describe("getRange", () => {
  test("offset+length slice of a numeric chunk is correct", async () => {
    // get the full chunk first, then compare to getRange
    const full = await store.get("/n_counts/0");
    expect(full).toBeDefined();
    // read bytes 4..12 (floats at index 1 and 2)
    const ranged = await store.getRange("/n_counts/0", { offset: 4, length: 8 });
    expect(ranged).toBeDefined();
    expect(Array.from(ranged!)).toEqual(Array.from(full!.slice(4, 12)));
  });

  test("undefined key returns undefined from getRange", async () => {
    const result = await store.getRange("/nonexistent/0", { offset: 0, length: 4 });
    expect(result).toBeUndefined();
  });
});

describe("unknown keys return undefined", () => {
  test("unknown column", async () => {
    expect(await store.get("/nonexistent/.zattrs")).toBeUndefined();
  });

  test("unknown subkey", async () => {
    expect(await store.get("/n_counts/.zgroup")).toBeUndefined();
  });

  test("out-of-range row group", async () => {
    const numRgs = parquetMeta.row_groups.length;
    expect(await store.get(`/n_counts/${numRgs}` as `/${string}`)).toBeUndefined();
  });
});
