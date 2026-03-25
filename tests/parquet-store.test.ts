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

/** Read and parse a zarr.json file from the ground-truth zarr store. */
async function zarrMeta(subpath: string): Promise<Record<string, unknown>> {
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

describe("root /zarr.json", () => {
  test("zarr_format is 3 and node_type is group", async () => {
    const virtual = await getJson(store, "/zarr.json");
    expect(virtual.zarr_format).toBe(3);
    expect(virtual.node_type).toBe("group");
  });

  test("attributes encoding-type and encoding-version match zarr", async () => {
    const virtual = await getJson(store, "/zarr.json");
    const actual = await zarrMeta("zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
    expect(vAttrs["encoding-version"]).toBe(aAttrs["encoding-version"]);
  });

  test("attributes column-order matches zarr", async () => {
    const virtual = await getJson(store, "/zarr.json");
    const actual = await zarrMeta("zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["column-order"]).toEqual(aAttrs["column-order"]);
  });

  test("attributes _index matches zarr", async () => {
    const virtual = await getJson(store, "/zarr.json");
    const actual = await zarrMeta("zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["_index"]).toBe(aAttrs["_index"]);
  });
});

describe("numeric column n_counts", () => {
  test("zarr.json data_type matches zarr", async () => {
    const virtual = await getJson(store, "/n_counts/zarr.json");
    const actual = await zarrMeta("n_counts/zarr.json");
    expect(virtual.data_type).toBe(actual.data_type);
  });

  test("zarr.json zarr_format is 3 and node_type is array", async () => {
    const virtual = await getJson(store, "/n_counts/zarr.json");
    expect(virtual.zarr_format).toBe(3);
    expect(virtual.node_type).toBe("array");
  });

  test("zarr.json shape covers all rows", async () => {
    const virtual = await getJson(store, "/n_counts/zarr.json");
    const numRows = parquetMeta.row_groups.reduce(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (s: number, rg: any) => s + Number(rg.num_rows),
      0
    );
    const shape = virtual.shape as number[];
    expect(shape[0]).toBe(numRows);
  });

  test("zarr.json attributes encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/n_counts/zarr.json");
    const actual = await zarrMeta("n_counts/zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
  });

  test("zarr.json codecs include bytes with little endian", async () => {
    const virtual = await getJson(store, "/n_counts/zarr.json");
    const codecs = virtual.codecs as Array<Record<string, unknown>>;
    const bytesCodec = codecs.find((c) => c.name === "bytes");
    expect(bytesCodec).toBeDefined();
    expect((bytesCodec!.configuration as Record<string, unknown>)?.endian).toBe("little");
  });
});

describe("numeric column n_genes", () => {
  test("zarr.json data_type matches zarr", async () => {
    const virtual = await getJson(store, "/n_genes/zarr.json");
    const actual = await zarrMeta("n_genes/zarr.json");
    expect(virtual.data_type).toBe(actual.data_type);
  });

  test("zarr.json attributes encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/n_genes/zarr.json");
    const actual = await zarrMeta("n_genes/zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
  });
});

describe("index column obs_id", () => {
  test("zarr.json data_type matches zarr", async () => {
    const virtual = await getJson(store, "/obs_id/zarr.json");
    const actual = await zarrMeta("obs_id/zarr.json");
    expect(virtual.data_type).toBe(actual.data_type);
  });

  test("zarr.json attributes encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/obs_id/zarr.json");
    const actual = await zarrMeta("obs_id/zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
  });

  test("zarr.json codecs include vlen-utf8", async () => {
    const virtual = await getJson(store, "/obs_id/zarr.json");
    const codecs = virtual.codecs as Array<Record<string, unknown>>;
    expect(codecs.some((c) => c.name === "vlen-utf8")).toBe(true);
  });
});

describe("categorical column cell_type", () => {
  test("zarr.json is a group with categorical encoding-type", async () => {
    const virtual = await getJson(store, "/cell_type/zarr.json");
    const actual = await zarrMeta("cell_type/zarr.json");
    expect(virtual.node_type).toBe("group");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
    expect(vAttrs["encoding-version"]).toBe(aAttrs["encoding-version"]);
  });

  test("zarr.json attributes ordered matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/zarr.json");
    const actual = await zarrMeta("cell_type/zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs.ordered).toBe(aAttrs.ordered);
  });

  test("codes zarr.json data_type matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/codes/zarr.json");
    const actual = await zarrMeta("cell_type/codes/zarr.json");
    expect(virtual.data_type).toBe(actual.data_type);
  });

  test("codes zarr.json zarr_format is 3", async () => {
    const virtual = await getJson(store, "/cell_type/codes/zarr.json");
    expect(virtual.zarr_format).toBe(3);
  });

  test("categories zarr.json data_type matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/categories/zarr.json");
    const actual = await zarrMeta("cell_type/categories/zarr.json");
    expect(virtual.data_type).toBe(actual.data_type);
  });

  test("categories zarr.json shape matches zarr", async () => {
    const virtual = await getJson(store, "/cell_type/categories/zarr.json");
    const actual = await zarrMeta("cell_type/categories/zarr.json");
    expect(virtual.shape).toEqual(actual.shape);
  });
});

describe("categorical column leiden", () => {
  test("zarr.json attributes encoding-type matches zarr", async () => {
    const virtual = await getJson(store, "/leiden/zarr.json");
    const actual = await zarrMeta("leiden/zarr.json");
    const vAttrs = virtual.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
  });

  test("codes zarr.json data_type matches zarr", async () => {
    const virtual = await getJson(store, "/leiden/codes/zarr.json");
    const actual = await zarrMeta("leiden/codes/zarr.json");
    expect(virtual.data_type).toBe(actual.data_type);
  });

  test("categories zarr.json shape matches zarr", async () => {
    const virtual = await getJson(store, "/leiden/categories/zarr.json");
    const actual = await zarrMeta("leiden/categories/zarr.json");
    expect(virtual.shape).toEqual(actual.shape);
  });
});

// ── array data tests ────────────────────────────────────────────────────────

describe("n_counts array data", () => {
  test("all row groups values match parquet", async () => {
    const numRgs = parquetMeta.row_groups.length;
    const virtualValues: number[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/n_counts/c/${rg}` as `/${string}`);
      expect(bytes).toBeDefined();
      const floats = new Float32Array(
        bytes!.buffer,
        bytes!.byteOffset,
        bytes!.byteLength / 4
      );
      virtualValues.push(...Array.from(floats));
    }
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
      const bytes = await store.get(`/n_genes/c/${rg}` as `/${string}`);
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
      const bytes = await store.get(`/obs_id/c/${rg}` as `/${string}`);
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
    const bytes = await store.get("/cell_type/categories/c/0");
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
    const catBytes = await store.get("/cell_type/categories/c/0");
    expect(catBytes).toBeDefined();
    const categories = decodeVlenUtf8(catBytes!);

    const numRgs = parquetMeta.row_groups.length;
    const decoded: string[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/cell_type/codes/c/${rg}` as `/${string}`);
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
    const bytes = await store.get("/leiden/categories/c/0");
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
    const catBytes = await store.get("/leiden/categories/c/0");
    expect(catBytes).toBeDefined();
    const categories = decodeVlenUtf8(catBytes!);

    const numRgs = parquetMeta.row_groups.length;
    const decoded: string[] = [];
    for (let rg = 0; rg < numRgs; rg++) {
      const bytes = await store.get(`/leiden/codes/c/${rg}` as `/${string}`);
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
    const full = await store.get("/n_counts/c/0");
    expect(full).toBeDefined();
    const ranged = await store.getRange("/n_counts/c/0", { offset: 4, length: 8 });
    expect(ranged).toBeDefined();
    expect(Array.from(ranged!)).toEqual(Array.from(full!.slice(4, 12)));
  });

  test("undefined key returns undefined from getRange", async () => {
    const result = await store.getRange("/nonexistent/c/0", { offset: 0, length: 4 });
    expect(result).toBeUndefined();
  });
});

// ── consolidated metadata tests ─────────────────────────────────────────────

describe("consolidated metadata", () => {
  test("has correct top-level structure", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    expect(consolidated.zarr_format).toBe(3);
    expect(consolidated.node_type).toBe("group");
    expect(consolidated.consolidated_metadata).toBeDefined();
    const cm = consolidated.consolidated_metadata as Record<string, unknown>;
    expect(cm.kind).toBe("inline");
    expect(cm.must_understand).toBe(false);
    expect(cm.metadata).toBeDefined();
  });

  test("attributes match ground-truth zarr.json", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const actual = await zarrMeta("zarr.json");
    const vAttrs = consolidated.attributes as Record<string, unknown>;
    const aAttrs = actual.attributes as Record<string, unknown>;
    expect(vAttrs["column-order"]).toEqual(aAttrs["column-order"]);
    expect(vAttrs["_index"]).toBe(aAttrs["_index"]);
    expect(vAttrs["encoding-type"]).toBe(aAttrs["encoding-type"]);
    expect(vAttrs["encoding-version"]).toBe(aAttrs["encoding-version"]);
  });

  test("lists all expected metadata keys", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const cm = consolidated.consolidated_metadata as Record<string, Record<string, unknown>>;
    const keys = Object.keys(cm.metadata).sort();
    expect(keys).toEqual([
      "cell_type",
      "cell_type/categories",
      "cell_type/codes",
      "leiden",
      "leiden/categories",
      "leiden/codes",
      "n_counts",
      "n_genes",
      "obs_id",
    ]);
  });

  test("numeric column metadata has correct data_type", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const cm = consolidated.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    const nCounts = cm.metadata["n_counts"];
    expect(nCounts.node_type).toBe("array");
    expect(nCounts.data_type).toBe("float32");
  });

  test("categorical group has consolidated_metadata with empty metadata", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const cm = consolidated.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    const cellType = cm.metadata["cell_type"];
    expect(cellType.node_type).toBe("group");
    const innerCm = cellType.consolidated_metadata as Record<string, unknown>;
    expect(innerCm.kind).toBe("inline");
    expect(innerCm.metadata).toEqual({});
  });

  test("categorical codes metadata has correct data_type", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const cm = consolidated.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    const codes = cm.metadata["cell_type/codes"];
    expect(codes.node_type).toBe("array");
    expect(codes.data_type).toBe("int8");
  });

  test("categorical categories metadata has correct data_type and shape", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const cm = consolidated.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    const cats = cm.metadata["cell_type/categories"];
    expect(cats.node_type).toBe("array");
    expect(cats.data_type).toBe("string");
    expect(cats.shape).toEqual([3]);
  });

  test("string index column metadata has correct data_type", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const cm = consolidated.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    const obsId = cm.metadata["obs_id"];
    expect(obsId.node_type).toBe("array");
    expect(obsId.data_type).toBe("string");
  });
});

describe("unknown keys return undefined", () => {
  test("unknown column", async () => {
    expect(await store.get("/nonexistent/zarr.json")).toBeUndefined();
  });

  test("unknown subkey", async () => {
    // non-categorical column shouldn't have a group-like subpath
    expect(await store.get("/n_counts/codes/zarr.json")).toBeUndefined();
  });

  test("out-of-range row group", async () => {
    const numRgs = parquetMeta.row_groups.length;
    expect(await store.get(`/n_counts/c/${numRgs}` as `/${string}`)).toBeUndefined();
  });
});
