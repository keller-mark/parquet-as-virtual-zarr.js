import { describe, test, expect, beforeAll } from "vitest";
import {
  tableFromArrays,
  tableToIPC,
  dictionary,
  utf8,
  float32,
  int32,
} from "@uwdata/flechette";
import { ArrowAsAnnDataFrameStore } from "../src/arrow-store.js";

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

async function getJson(
  store: ArrowAsAnnDataFrameStore,
  key: `/${string}`
): Promise<Record<string, unknown>> {
  const bytes = await store.get(key);
  if (!bytes) throw new Error(`store returned undefined for ${key}`);
  return JSON.parse(new TextDecoder().decode(bytes));
}

// ── test data ─────────────────────────────────────────────────────────────

const NUM_ROWS = 20;
const obsIds = Array.from({ length: NUM_ROWS }, (_, i) => `cell_${String(i).padStart(3, "0")}`);
const nCounts = Float32Array.from({ length: NUM_ROWS }, (_, i) => (i + 1) * 1.5);
const nGenes = Int32Array.from({ length: NUM_ROWS }, (_, i) => (i + 1) * 10);
const cellTypeValues = ["TypeA", "TypeB", "TypeC"];
const cellTypes = Array.from({ length: NUM_ROWS }, (_, i) => cellTypeValues[i % 3]);
const leidenValues = ["0", "1", "2", "3", "4"];
const leiden = Array.from({ length: NUM_ROWS }, (_, i) => leidenValues[i % 5]);

// ── fixtures ──────────────────────────────────────────────────────────────

let store: ArrowAsAnnDataFrameStore;
let storeWithPandasMeta: ArrowAsAnnDataFrameStore;

beforeAll(() => {
  // Build table without pandas metadata (uses Arrow dictionary detection)
  const table = tableFromArrays({
    obs_id: obsIds,
    n_counts: nCounts,
    n_genes: nGenes,
    cell_type: cellTypes,
    leiden,
  }, {
    types: {
      obs_id: utf8(),
      n_counts: float32(),
      n_genes: int32(),
      cell_type: dictionary(utf8()),
      leiden: dictionary(utf8()),
    },
  });
  const ipcBytes = tableToIPC(table, { format: "file" });
  store = ArrowAsAnnDataFrameStore.fromIPC(ipcBytes!);

  // Build table with pandas metadata embedded in schema
  const tableWithMeta = tableFromArrays({
    obs_id: obsIds,
    n_counts: nCounts,
    n_genes: nGenes,
    cell_type: cellTypes,
    leiden,
  }, {
    types: {
      obs_id: utf8(),
      n_counts: float32(),
      n_genes: int32(),
      cell_type: dictionary(utf8()),
      leiden: dictionary(utf8()),
    },
  });
  // Inject pandas metadata into the schema
  if (!tableWithMeta.schema.metadata) {
    tableWithMeta.schema.metadata = new Map();
  }
  tableWithMeta.schema.metadata.set("pandas", JSON.stringify({
    index_columns: ["obs_id"],
    columns: [
      { name: "obs_id", pandas_type: "unicode", numpy_type: "object" },
      { name: "n_counts", pandas_type: "float32", numpy_type: "float32" },
      { name: "n_genes", pandas_type: "int32", numpy_type: "int32" },
      { name: "cell_type", pandas_type: "categorical", numpy_type: "object" },
      { name: "leiden", pandas_type: "categorical", numpy_type: "object" },
    ],
  }));
  const ipcBytesWithMeta = tableToIPC(tableWithMeta, { format: "file" });
  storeWithPandasMeta = ArrowAsAnnDataFrameStore.fromIPC(ipcBytesWithMeta!);
});

// ── metadata tests ────────────────────────────────────────────────────────

describe("root /zarr.json", () => {
  test("zarr_format is 3 and node_type is group", async () => {
    const meta = await getJson(store, "/zarr.json");
    expect(meta.zarr_format).toBe(3);
    expect(meta.node_type).toBe("group");
  });

  test("attributes encoding-type is dataframe", async () => {
    const meta = await getJson(store, "/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["encoding-type"]).toBe("dataframe");
    expect(attrs["encoding-version"]).toBe("0.2.0");
  });

  test("attributes _index is first column when no pandas metadata", async () => {
    const meta = await getJson(store, "/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["_index"]).toBe("obs_id");
  });

  test("column-order excludes index column", async () => {
    const meta = await getJson(store, "/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    const order = attrs["column-order"] as string[];
    expect(order).toEqual(["n_counts", "n_genes", "cell_type", "leiden"]);
    expect(order).not.toContain("obs_id");
  });
});

describe("root /zarr.json with pandas metadata", () => {
  test("_index comes from pandas metadata", async () => {
    const meta = await getJson(storeWithPandasMeta, "/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["_index"]).toBe("obs_id");
  });
});

describe("numeric column n_counts", () => {
  test("zarr.json data_type is float32", async () => {
    const meta = await getJson(store, "/n_counts/zarr.json");
    expect(meta.data_type).toBe("float32");
  });

  test("zarr.json zarr_format is 3 and node_type is array", async () => {
    const meta = await getJson(store, "/n_counts/zarr.json");
    expect(meta.zarr_format).toBe(3);
    expect(meta.node_type).toBe("array");
  });

  test("zarr.json shape covers all rows", async () => {
    const meta = await getJson(store, "/n_counts/zarr.json");
    expect((meta.shape as number[])[0]).toBe(NUM_ROWS);
  });

  test("zarr.json attributes encoding-type is array", async () => {
    const meta = await getJson(store, "/n_counts/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["encoding-type"]).toBe("array");
  });

  test("zarr.json codecs include bytes with little endian", async () => {
    const meta = await getJson(store, "/n_counts/zarr.json");
    const codecs = meta.codecs as Array<Record<string, unknown>>;
    const bytesCodec = codecs.find((c) => c.name === "bytes");
    expect(bytesCodec).toBeDefined();
    expect((bytesCodec!.configuration as Record<string, unknown>)?.endian).toBe("little");
  });
});

describe("numeric column n_genes", () => {
  test("zarr.json data_type is int32", async () => {
    const meta = await getJson(store, "/n_genes/zarr.json");
    expect(meta.data_type).toBe("int32");
  });

  test("zarr.json attributes encoding-type is array", async () => {
    const meta = await getJson(store, "/n_genes/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["encoding-type"]).toBe("array");
  });
});

describe("index column obs_id", () => {
  test("zarr.json data_type is string", async () => {
    const meta = await getJson(store, "/obs_id/zarr.json");
    expect(meta.data_type).toBe("string");
  });

  test("zarr.json attributes encoding-type is string-array", async () => {
    const meta = await getJson(store, "/obs_id/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["encoding-type"]).toBe("string-array");
  });

  test("zarr.json codecs include vlen-utf8", async () => {
    const meta = await getJson(store, "/obs_id/zarr.json");
    const codecs = meta.codecs as Array<Record<string, unknown>>;
    expect(codecs.some((c) => c.name === "vlen-utf8")).toBe(true);
  });
});

describe("categorical column cell_type", () => {
  test("zarr.json is a group with categorical encoding-type", async () => {
    const meta = await getJson(store, "/cell_type/zarr.json");
    expect(meta.node_type).toBe("group");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["encoding-type"]).toBe("categorical");
    expect(attrs["encoding-version"]).toBe("0.2.0");
  });

  test("zarr.json attributes ordered is false", async () => {
    const meta = await getJson(store, "/cell_type/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs.ordered).toBe(false);
  });

  test("codes zarr.json data_type is int8", async () => {
    const meta = await getJson(store, "/cell_type/codes/zarr.json");
    expect(meta.data_type).toBe("int8");
  });

  test("codes zarr.json zarr_format is 3", async () => {
    const meta = await getJson(store, "/cell_type/codes/zarr.json");
    expect(meta.zarr_format).toBe(3);
  });

  test("categories zarr.json data_type is string", async () => {
    const meta = await getJson(store, "/cell_type/categories/zarr.json");
    expect(meta.data_type).toBe("string");
  });

  test("categories zarr.json shape matches number of unique values", async () => {
    const meta = await getJson(store, "/cell_type/categories/zarr.json");
    expect(meta.shape).toEqual([cellTypeValues.length]);
  });
});

describe("categorical column leiden", () => {
  test("zarr.json attributes encoding-type is categorical", async () => {
    const meta = await getJson(store, "/leiden/zarr.json");
    const attrs = meta.attributes as Record<string, unknown>;
    expect(attrs["encoding-type"]).toBe("categorical");
  });

  test("codes zarr.json data_type is int8", async () => {
    const meta = await getJson(store, "/leiden/codes/zarr.json");
    expect(meta.data_type).toBe("int8");
  });

  test("categories zarr.json shape matches number of unique values", async () => {
    const meta = await getJson(store, "/leiden/categories/zarr.json");
    expect(meta.shape).toEqual([leidenValues.length]);
  });
});

// ── array data tests ──────────────────────────────────────────────────────

describe("n_counts array data", () => {
  test("chunk values match input", async () => {
    const bytes = await store.get("/n_counts/c/0");
    expect(bytes).toBeDefined();
    const floats = new Float32Array(
      bytes!.buffer,
      bytes!.byteOffset,
      bytes!.byteLength / 4
    );
    expect(Array.from(floats)).toEqual(Array.from(nCounts));
  });
});

describe("n_genes array data", () => {
  test("chunk values match input", async () => {
    const bytes = await store.get("/n_genes/c/0");
    expect(bytes).toBeDefined();
    const ints = new Int32Array(
      bytes!.buffer,
      bytes!.byteOffset,
      bytes!.byteLength / 4
    );
    expect(Array.from(ints)).toEqual(Array.from(nGenes));
  });
});

describe("obs_id string array data", () => {
  test("chunk values match input", async () => {
    const bytes = await store.get("/obs_id/c/0");
    expect(bytes).toBeDefined();
    const decoded = decodeVlenUtf8(bytes!);
    expect(decoded).toEqual(obsIds);
  });
});

describe("cell_type categorical data", () => {
  test("categories are sorted unique values", async () => {
    const bytes = await store.get("/cell_type/categories/c/0");
    expect(bytes).toBeDefined();
    const categories = decodeVlenUtf8(bytes!);
    expect(categories).toEqual([...new Set(cellTypes)].sort());
  });

  test("codes round-trip back to original string values", async () => {
    const catBytes = await store.get("/cell_type/categories/c/0");
    expect(catBytes).toBeDefined();
    const categories = decodeVlenUtf8(catBytes!);

    const codeBytes = await store.get("/cell_type/codes/c/0");
    expect(codeBytes).toBeDefined();
    const codes = new Int8Array(codeBytes!.buffer, codeBytes!.byteOffset, codeBytes!.byteLength);
    const decoded = Array.from(codes).map((code) => categories[code]);
    expect(decoded).toEqual(cellTypes);
  });
});

describe("leiden categorical data", () => {
  test("categories are sorted unique values", async () => {
    const bytes = await store.get("/leiden/categories/c/0");
    expect(bytes).toBeDefined();
    const categories = decodeVlenUtf8(bytes!);
    expect(categories).toEqual([...new Set(leiden)].sort());
  });

  test("codes round-trip back to original string values", async () => {
    const catBytes = await store.get("/leiden/categories/c/0");
    expect(catBytes).toBeDefined();
    const categories = decodeVlenUtf8(catBytes!);

    const codeBytes = await store.get("/leiden/codes/c/0");
    expect(codeBytes).toBeDefined();
    const codes = new Int8Array(codeBytes!.buffer, codeBytes!.byteOffset, codeBytes!.byteLength);
    const decoded = Array.from(codes).map((code) => categories[code]);
    expect(decoded).toEqual(leiden);
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

  test("suffixLength slice works correctly", async () => {
    const full = await store.get("/n_counts/c/0");
    expect(full).toBeDefined();
    const ranged = await store.getRange("/n_counts/c/0", { suffixLength: 8 });
    expect(ranged).toBeDefined();
    expect(Array.from(ranged!)).toEqual(Array.from(full!.slice(full!.byteLength - 8)));
  });

  test("undefined key returns undefined from getRange", async () => {
    const result = await store.getRange("/nonexistent/c/0", { offset: 0, length: 4 });
    expect(result).toBeUndefined();
  });
});

// ── consolidated metadata tests ───────────────────────────────────────────

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
    expect(cm.metadata["n_counts"].data_type).toBe("float32");
    expect(cm.metadata["n_genes"].data_type).toBe("int32");
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

  test("string index column metadata has correct data_type", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const cm = consolidated.consolidated_metadata as Record<string, Record<string, Record<string, unknown>>>;
    expect(cm.metadata["obs_id"].data_type).toBe("string");
  });
});

describe("unknown keys return undefined", () => {
  test("unknown column", async () => {
    expect(await store.get("/nonexistent/zarr.json")).toBeUndefined();
  });

  test("unknown subkey on non-categorical column", async () => {
    expect(await store.get("/n_counts/codes/zarr.json")).toBeUndefined();
  });

  test("out-of-range chunk index", async () => {
    expect(await store.get("/n_counts/c/999")).toBeUndefined();
  });
});
