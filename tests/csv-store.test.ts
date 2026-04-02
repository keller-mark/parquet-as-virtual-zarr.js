import { describe, test, expect, beforeAll } from "vitest";
import { open, get, root } from "zarrita";
import { CsvAsAnnDataFrameStore } from "../src/csv-store.js";

// ── helpers ────────────────────────────────────────────────────────────────

async function getJson(
  store: CsvAsAnnDataFrameStore,
  key: `/${string}`
): Promise<Record<string, unknown>> {
  const bytes = await store.get(key);
  if (!bytes) throw new Error(`store returned undefined for ${key}`);
  return JSON.parse(new TextDecoder().decode(bytes));
}

// ── test data ─────────────────────────────────────────────────────────────

const NUM_ROWS = 20;
const obsIds = Array.from({ length: NUM_ROWS }, (_, i) => `cell_${String(i).padStart(3, "0")}`);
const nCounts = Array.from({ length: NUM_ROWS }, (_, i) => (i + 1) * 1.5);
const nGenes = Array.from({ length: NUM_ROWS }, (_, i) => (i + 1) * 10);
const cellTypeValues = ["TypeA", "TypeB", "TypeC"];
const cellTypes = Array.from({ length: NUM_ROWS }, (_, i) => cellTypeValues[i % 3]);
const leidenValues = ["c0", "c1", "c2", "c3", "c4"];
const leiden = Array.from({ length: NUM_ROWS }, (_, i) => leidenValues[i % 5]);

function buildCsv(sep = ","): string {
  const header = ["obs_id", "n_counts", "n_genes", "cell_type", "leiden"].join(sep);
  const rows = Array.from({ length: NUM_ROWS }, (_, i) =>
    [obsIds[i], nCounts[i], nGenes[i], cellTypes[i], leiden[i]].join(sep)
  );
  return [header, ...rows].join("\n");
}

// ── fixtures ───────────────────────────────────────────────────────────────

let store: CsvAsAnnDataFrameStore;
let tsvStore: CsvAsAnnDataFrameStore;
let chunkedStore: CsvAsAnnDataFrameStore;

beforeAll(() => {
  store = CsvAsAnnDataFrameStore.fromText(buildCsv());
  tsvStore = CsvAsAnnDataFrameStore.fromText(buildCsv("\t"), { separator: "\t" });
  chunkedStore = CsvAsAnnDataFrameStore.fromText(buildCsv(), { chunkSize: 5 });
});

// ── root metadata ─────────────────────────────────────────────────────────

describe("root /zarr.json", () => {
  test("zarr_format is 3 and node_type is group", async () => {
    const grp = await open(root(store), { kind: "group" });
    expect(grp.kind).toBe("group");
  });

  test("encoding-type is dataframe", async () => {
    const grp = await open(root(store), { kind: "group" });
    expect(grp.attrs["encoding-type"]).toBe("dataframe");
    expect(grp.attrs["encoding-version"]).toBe("0.2.0");
  });

  test("_index defaults to first column", async () => {
    const grp = await open(root(store), { kind: "group" });
    expect(grp.attrs["_index"]).toBe("obs_id");
  });

  test("column-order excludes index column", async () => {
    const grp = await open(root(store), { kind: "group" });
    const order = grp.attrs["column-order"] as string[];
    expect(order).toEqual(["n_counts", "n_genes", "cell_type", "leiden"]);
    expect(order).not.toContain("obs_id");
  });
});

// ── type auto-detection ───────────────────────────────────────────────────

describe("auto-detected column types", () => {
  test("integer column detected as int32", async () => {
    const arr = await open(root(store).resolve("n_genes"), { kind: "array" });
    expect(arr.dtype).toBe("int32");
  });

  test("float column detected as float64", async () => {
    const arr = await open(root(store).resolve("n_counts"), { kind: "array" });
    expect(arr.dtype).toBe("float64");
  });

  test("index string column exposed as string", async () => {
    const arr = await open(root(store).resolve("obs_id"), { kind: "array" });
    expect(arr.dtype).toBe("string");
  });

  test("low-cardinality string column auto-detected as categorical", async () => {
    const grp = await open(root(store).resolve("cell_type"), { kind: "group" });
    expect(grp.attrs["encoding-type"]).toBe("categorical");
  });

  test("leiden auto-detected as categorical", async () => {
    const grp = await open(root(store).resolve("leiden"), { kind: "group" });
    expect(grp.attrs["encoding-type"]).toBe("categorical");
  });
});

// ── numeric column metadata ───────────────────────────────────────────────

describe("numeric column n_genes", () => {
  test("zarr.json shape covers all rows", async () => {
    const arr = await open(root(store).resolve("n_genes"), { kind: "array" });
    expect(arr.shape[0]).toBe(NUM_ROWS);
  });

  test("zarr.json attributes encoding-type is array", async () => {
    const arr = await open(root(store).resolve("n_genes"), { kind: "array" });
    expect(arr.attrs["encoding-type"]).toBe("array");
    expect(arr.attrs["encoding-version"]).toBe("0.2.0");
  });

  test("zarr.json codecs include bytes with little endian", async () => {
    const meta = await getJson(store, "/n_genes/zarr.json");
    const codecs = meta.codecs as Array<Record<string, unknown>>;
    const bytesCodec = codecs.find(c => c.name === "bytes");
    expect(bytesCodec).toBeDefined();
    expect((bytesCodec!.configuration as Record<string, unknown>)?.endian).toBe("little");
  });
});

describe("float column n_counts", () => {
  test("zarr.json data_type is float64", async () => {
    const arr = await open(root(store).resolve("n_counts"), { kind: "array" });
    expect(arr.dtype).toBe("float64");
  });
});

// ── string / index column ─────────────────────────────────────────────────

describe("index column obs_id", () => {
  test("zarr.json data_type is string", async () => {
    const arr = await open(root(store).resolve("obs_id"), { kind: "array" });
    expect(arr.dtype).toBe("string");
  });

  test("zarr.json attributes encoding-type is string-array", async () => {
    const arr = await open(root(store).resolve("obs_id"), { kind: "array" });
    expect(arr.attrs["encoding-type"]).toBe("string-array");
  });

  test("zarr.json codecs include vlen-utf8", async () => {
    const meta = await getJson(store, "/obs_id/zarr.json");
    const codecs = meta.codecs as Array<Record<string, unknown>>;
    expect(codecs.some(c => c.name === "vlen-utf8")).toBe(true);
  });
});

// ── categorical column metadata ───────────────────────────────────────────

describe("categorical column cell_type", () => {
  test("zarr.json is a group with categorical encoding-type", async () => {
    const grp = await open(root(store).resolve("cell_type"), { kind: "group" });
    expect(grp.kind).toBe("group");
    expect(grp.attrs["encoding-type"]).toBe("categorical");
    expect(grp.attrs["encoding-version"]).toBe("0.2.0");
  });

  test("zarr.json attributes ordered is false", async () => {
    const grp = await open(root(store).resolve("cell_type"), { kind: "group" });
    expect(grp.attrs.ordered).toBe(false);
  });

  test("codes zarr.json data_type is int8 (3 categories ≤ 128)", async () => {
    const arr = await open(root(store).resolve("cell_type/codes"), { kind: "array" });
    expect(arr.dtype).toBe("int8");
  });

  test("categories zarr.json data_type is string", async () => {
    const arr = await open(root(store).resolve("cell_type/categories"), { kind: "array" });
    expect(arr.dtype).toBe("string");
  });

  test("categories zarr.json shape matches number of unique values", async () => {
    const arr = await open(root(store).resolve("cell_type/categories"), { kind: "array" });
    expect(arr.shape).toEqual([cellTypeValues.length]);
  });
});

// ── array data ────────────────────────────────────────────────────────────

describe("n_genes array data", () => {
  test("chunk values match input", async () => {
    const arr = await open(root(store).resolve("n_genes"), { kind: "array" });
    const chunk = await get(arr);
    expect(Array.from(chunk.data as Int32Array)).toEqual(nGenes);
  });
});

describe("n_counts array data", () => {
  test("chunk values match input", async () => {
    const arr = await open(root(store).resolve("n_counts"), { kind: "array" });
    const chunk = await get(arr);
    expect(Array.from(chunk.data as Float64Array)).toEqual(nCounts);
  });
});

describe("obs_id string array data", () => {
  test("chunk values match input", async () => {
    const arr = await open(root(store).resolve("obs_id"), { kind: "array" });
    const chunk = await get(arr);
    expect(chunk.data as string[]).toEqual(obsIds);
  });
});

describe("cell_type categorical data", () => {
  test("categories are sorted unique values", async () => {
    const catsArr = await open(root(store).resolve("cell_type/categories"), { kind: "array" });
    const catsChunk = await get(catsArr);
    expect(catsChunk.data as string[]).toEqual([...new Set(cellTypes)].sort());
  });

  test("codes round-trip back to original string values", async () => {
    const catsArr = await open(root(store).resolve("cell_type/categories"), { kind: "array" });
    const catsChunk = await get(catsArr);
    const categories = catsChunk.data as string[];

    const codesArr = await open(root(store).resolve("cell_type/codes"), { kind: "array" });
    const codesChunk = await get(codesArr);
    const decoded = Array.from(codesChunk.data as Int8Array, code => categories[code]);
    expect(decoded).toEqual(cellTypes);
  });
});

describe("leiden categorical data", () => {
  test("categories are sorted unique values", async () => {
    const catsArr = await open(root(store).resolve("leiden/categories"), { kind: "array" });
    const catsChunk = await get(catsArr);
    expect(catsChunk.data as string[]).toEqual([...new Set(leiden)].sort());
  });

  test("codes round-trip back to original string values", async () => {
    const catsArr = await open(root(store).resolve("leiden/categories"), { kind: "array" });
    const catsChunk = await get(catsArr);
    const categories = catsChunk.data as string[];

    const codesArr = await open(root(store).resolve("leiden/codes"), { kind: "array" });
    const codesChunk = await get(codesArr);
    const decoded = Array.from(codesChunk.data as Int8Array, code => categories[code]);
    expect(decoded).toEqual(leiden);
  });
});

// ── TSV support ───────────────────────────────────────────────────────────

describe("TSV support", () => {
  test("parses tab-separated file correctly", async () => {
    const grp = await open(root(tsvStore), { kind: "group" });
    expect(grp.attrs["encoding-type"]).toBe("dataframe");
    expect(grp.attrs["_index"]).toBe("obs_id");
  });

  test("n_genes values match input", async () => {
    const arr = await open(root(tsvStore).resolve("n_genes"), { kind: "array" });
    const chunk = await get(arr);
    expect(Array.from(chunk.data as Int32Array)).toEqual(nGenes);
  });
});

// ── Custom options ────────────────────────────────────────────────────────

describe("custom indexColumn", () => {
  test("uses specified index column", async () => {
    const s = CsvAsAnnDataFrameStore.fromText(buildCsv(), { indexColumn: "n_genes" });
    const grp = await open(root(s), { kind: "group" });
    expect(grp.attrs["_index"]).toBe("n_genes");
    const order = grp.attrs["column-order"] as string[];
    expect(order).not.toContain("n_genes");
    expect(order).toContain("obs_id");
  });
});

describe("custom columnTypes override", () => {
  test("forces a column to float32", async () => {
    const s = CsvAsAnnDataFrameStore.fromText(buildCsv(), {
      columnTypes: { n_genes: "float32" },
    });
    const arr = await open(root(s).resolve("n_genes"), { kind: "array" });
    expect(arr.dtype).toBe("float32");
    const chunk = await get(arr);
    expect(Array.from(chunk.data as Float32Array)).toEqual(nGenes.map(v => v));
  });

  test("forces a column to string (disabling auto-categorical)", async () => {
    const s = CsvAsAnnDataFrameStore.fromText(buildCsv(), {
      columnTypes: { cell_type: "string" },
    });
    const arr = await open(root(s).resolve("cell_type"), { kind: "array" });
    expect(arr.dtype).toBe("string");
    const chunk = await get(arr);
    expect(chunk.data as string[]).toEqual(cellTypes);
  });
});

// ── Multi-chunk support ───────────────────────────────────────────────────

describe("chunked store (chunkSize=5)", () => {
  test("chunk_shape in metadata is 5", async () => {
    const meta = await getJson(chunkedStore, "/n_genes/zarr.json");
    const chunkShape = (meta.chunk_grid as Record<string, Record<string, unknown>>)
      .configuration.chunk_shape as number[];
    expect(chunkShape).toEqual([5]);
  });

  test("all chunks together reconstruct the full n_genes array", async () => {
    const arr = await open(root(chunkedStore).resolve("n_genes"), { kind: "array" });
    const allValues: number[] = [];
    const numChunks = Math.ceil(NUM_ROWS / 5);
    for (let i = 0; i < numChunks; i++) {
      const bytes = await chunkedStore.get(`/n_genes/c/${i}`);
      expect(bytes).toBeDefined();
      const typed = new Int32Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength / 4);
      allValues.push(...typed);
    }
    expect(allValues).toEqual(nGenes);
    expect(arr.shape[0]).toBe(NUM_ROWS);
  });

  test("categorical codes reconstruct correctly across chunks", async () => {
    const catsArr = await open(root(chunkedStore).resolve("cell_type/categories"), { kind: "array" });
    const catsChunk = await get(catsArr);
    const categories = catsChunk.data as string[];

    const allDecoded: string[] = [];
    const numChunks = Math.ceil(NUM_ROWS / 5);
    for (let i = 0; i < numChunks; i++) {
      const bytes = await chunkedStore.get(`/cell_type/codes/c/${i}`);
      expect(bytes).toBeDefined();
      const typed = new Int8Array(bytes!.buffer, bytes!.byteOffset, bytes!.byteLength);
      allDecoded.push(...Array.from(typed, code => categories[code]));
    }
    expect(allDecoded).toEqual(cellTypes);
  });
});

// ── getRange ──────────────────────────────────────────────────────────────

describe("getRange", () => {
  test("offset+length slice of a numeric chunk is correct", async () => {
    const full = await store.get("/n_genes/c/0");
    expect(full).toBeDefined();
    const ranged = await store.getRange("/n_genes/c/0", { offset: 4, length: 8 });
    expect(ranged).toBeDefined();
    expect(Array.from(ranged!)).toEqual(Array.from(full!.slice(4, 12)));
  });

  test("suffixLength slice works correctly", async () => {
    const full = await store.get("/n_genes/c/0");
    expect(full).toBeDefined();
    const ranged = await store.getRange("/n_genes/c/0", { suffixLength: 8 });
    expect(ranged).toBeDefined();
    expect(Array.from(ranged!)).toEqual(Array.from(full!.slice(full!.byteLength - 8)));
  });

  test("undefined key returns undefined", async () => {
    const result = await store.getRange("/nonexistent/c/0", { offset: 0, length: 4 });
    expect(result).toBeUndefined();
  });
});

// ── consolidated metadata ─────────────────────────────────────────────────

describe("consolidated metadata", () => {
  test("has correct top-level structure", async () => {
    const cm = await store.getConsolidatedMetadata();
    expect(cm.zarr_format).toBe(3);
    expect(cm.node_type).toBe("group");
    const inner = cm.consolidated_metadata as Record<string, unknown>;
    expect(inner.kind).toBe("inline");
    expect(inner.must_understand).toBe(false);
    expect(inner.metadata).toBeDefined();
  });

  test("lists all expected metadata keys", async () => {
    const cm = await store.getConsolidatedMetadata();
    const inner = cm.consolidated_metadata as Record<string, Record<string, unknown>>;
    const keys = Object.keys(inner.metadata).sort();
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
    const cm = await store.getConsolidatedMetadata();
    const inner = cm.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    expect(inner.metadata["n_genes"].data_type).toBe("int32");
    expect(inner.metadata["n_counts"].data_type).toBe("float64");
  });

  test("categorical group has consolidated_metadata with empty metadata", async () => {
    const cm = await store.getConsolidatedMetadata();
    const inner = cm.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    const cellType = inner.metadata["cell_type"];
    expect(cellType.node_type).toBe("group");
    const innerCm = cellType.consolidated_metadata as Record<string, unknown>;
    expect(innerCm.kind).toBe("inline");
    expect(innerCm.metadata).toEqual({});
  });

  test("string index column metadata has correct data_type", async () => {
    const cm = await store.getConsolidatedMetadata();
    const inner = cm.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    expect(inner.metadata["obs_id"].data_type).toBe("string");
  });
});

// ── unknown keys ──────────────────────────────────────────────────────────

describe("unknown keys return undefined", () => {
  test("unknown column", async () => {
    expect(await store.get("/nonexistent/zarr.json")).toBeUndefined();
  });

  test("unknown subkey on non-categorical column", async () => {
    expect(await store.get("/n_genes/codes/zarr.json")).toBeUndefined();
  });

  test("out-of-range chunk index", async () => {
    expect(await store.get("/n_genes/c/999")).toBeUndefined();
  });

  test("categories chunk index > 0 returns undefined", async () => {
    expect(await store.get("/cell_type/categories/c/1")).toBeUndefined();
  });
});
