/**
 * Regression tests for edge cases of the virtual AnnData mapping (see SPEC.md):
 *
 * Parquet (fixtures/output/obs_edge.parquet vs adata_edge.zarr):
 *   - numeric (int64) index is exposed as strings
 *   - REQUIRED BOOLEAN column is a bool array
 *   - OPTIONAL FLOAT column is a plain float array with NaN for nulls
 *   - missing categorical values have code -1
 * Parquet zero-copy eligibility:
 *   - column chunks with several data pages are not passed through compressed
 *   - parts with different codecs are not passed through compressed
 * Parquet row-group regularity:
 *   - irregular row groups are rejected
 * Arrow:
 *   - missing categorical values have code -1
 *   - unsigned dictionary indices keep their type on the zero-copy path
 *   - half-precision floats are widened to float32
 */

import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import { describe, test, expect } from "vitest";
import FileSystemStore from "@zarrita/storage/fs";
import { open, get, root } from "zarrita";
import { tableFromArrays, tableToIPC, dictionary, utf8, uint8, float16, int32 } from "@uwdata/flechette";
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";
import { ArrowAsAnnDataFrameStore } from "../src/arrow-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../fixtures/output");

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Readable = any;

async function readAll(store: Readable, path: string): Promise<unknown[]> {
  const arr = await open(root(store).resolve(path), { kind: "array" });
  return Array.from((await get(arr)).data as Iterable<unknown>);
}

async function getJson(store: Readable, key: string): Promise<Record<string, unknown>> {
  return JSON.parse(new TextDecoder().decode(await store.get(key)));
}

// ── Parquet edge cases vs AnnData-Zarr ground truth ──────────────────────────

describe("parquet edge cases (obs_edge.parquet)", () => {
  const store = ParquetAsAnnDataFrameStore.fromStore(new FileSystemStore(resolve(OUT, "obs_edge.parquet")));
  const zarrStore = new FileSystemStore(resolve(OUT, "adata_edge.zarr/obs"));

  test("root attributes match zarr", async () => {
    const virtual = await open(root(store), { kind: "group" });
    const expected = await open(root(zarrStore), { kind: "group" });
    expect(virtual.attrs).toEqual(expected.attrs);
  });

  test("numeric index is a string array whose values match zarr", async () => {
    const arr = await open(root(store).resolve("cell_id"), { kind: "array" });
    expect(arr.dtype).toBe("string");
    expect(await readAll(store, "cell_id")).toEqual(await readAll(zarrStore, "cell_id"));
  });

  test("REQUIRED bool column is a bool array matching zarr", async () => {
    const arr = await open(root(store).resolve("is_doublet"), { kind: "array" });
    expect(arr.dtype).toBe("bool");
    expect(arr.attrs["encoding-type"]).toBe("array");
    expect(await readAll(store, "is_doublet")).toEqual(await readAll(zarrStore, "is_doublet"));
  });

  test("OPTIONAL float column is a plain float array with NaN, matching zarr", async () => {
    const arr = await open(root(store).resolve("n_counts"), { kind: "array" });
    expect(arr.dtype).toBe("float32");
    const values = await readAll(store, "n_counts");
    expect(values.some((v) => Number.isNaN(v))).toBe(true);
    expect(values).toEqual(await readAll(zarrStore, "n_counts"));
  });

  for (const [col, encodingType] of [
    ["n_genes", "nullable-integer"],
    ["is_filtered", "nullable-boolean"],
    ["note", "nullable-string-array"],
  ]) {
    test(`OPTIONAL ${col} is a ${encodingType} group whose attributes match zarr`, async () => {
      const virtual = await open(root(store).resolve(col), { kind: "group" });
      const expected = await open(root(zarrStore).resolve(col), { kind: "group" });
      expect(virtual.attrs["encoding-type"]).toBe(encodingType);
      expect(virtual.attrs).toEqual(expected.attrs);
    });

    for (const member of ["values", "mask"]) {
      test(`${col}/${member} dtype, attributes, and data match zarr`, async () => {
        const virtual = await open(root(store).resolve(`${col}/${member}`), { kind: "array" });
        const expected = await open(root(zarrStore).resolve(`${col}/${member}`), { kind: "array" });
        expect(virtual.dtype).toBe(expected.dtype);
        expect(virtual.shape).toEqual(expected.shape);
        expect(virtual.attrs).toEqual(expected.attrs);
        expect(await readAll(store, `${col}/${member}`)).toEqual(await readAll(zarrStore, `${col}/${member}`));
      });
    }
  }

  test("nullable columns have missing values", async () => {
    for (const col of ["n_genes", "is_filtered", "note"]) {
      expect(await readAll(store, `${col}/mask`)).toContain(true);
    }
  });

  test("categorical with missing values: codes (-1 for missing) and categories match zarr", async () => {
    const codes = await readAll(store, "cell_type/codes");
    expect(codes).toContain(-1);
    expect(codes).toEqual(await readAll(zarrStore, "cell_type/codes"));
    expect(await readAll(store, "cell_type/categories")).toEqual(
      await readAll(zarrStore, "cell_type/categories"),
    );
  });
});

// ── Parquet zero-copy eligibility ────────────────────────────────────────────

describe("parquet zero-copy eligibility", () => {
  const zarrPlain = new FileSystemStore(resolve(OUT, "adata_plain.zarr/obs"));

  for (const [label, path] of [
    ["several data pages per column chunk", "obs_plain_multipage.parquet"],
    ["parts with different codecs", "obs_plain_mixed_codec"],
  ]) {
    test(`${label}: no compression codec declared, values match zarr`, async () => {
      const store = ParquetAsAnnDataFrameStore.fromStore(new FileSystemStore(resolve(OUT, path)));
      for (const col of ["n_counts", "n_genes"]) {
        const meta = await getJson(store, `/${col}/zarr.json`);
        expect((meta.codecs as { name: string }[]).map((c) => c.name)).toEqual(["bytes"]);
        expect(await readAll(store, col)).toEqual(await readAll(zarrPlain, col));
      }
    });
  }

  test("single-page compressed columns still use zero-copy pass-through", async () => {
    const store = ParquetAsAnnDataFrameStore.fromStore(
      new FileSystemStore(resolve(OUT, "obs_plain_snappy.parquet")),
    );
    const meta = await getJson(store, "/n_counts/zarr.json");
    expect((meta.codecs as { name: string }[]).map((c) => c.name)).toEqual(["bytes", "snappy"]);
  });
});

// ── Parquet row-group regularity ─────────────────────────────────────────────

describe("parquet row-group regularity", () => {
  test("irregular row groups are rejected on open", async () => {
    const store = ParquetAsAnnDataFrameStore.fromStore(
      new FileSystemStore(resolve(OUT, "obs_irregular.parquet")),
    );
    await expect(store.get("/zarr.json")).rejects.toThrow(/row group 1 has 30 rows/);
  });
});

// ── Arrow edge cases ─────────────────────────────────────────────────────────

function arrowStore(
  columns: Record<string, unknown[]>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  types: Record<string, any>,
  options: { maxBatchRows?: number; pandas?: unknown } = {},
): ArrowAsAnnDataFrameStore {
  const table = tableFromArrays(columns, { types, maxBatchRows: options.maxBatchRows });
  if (options.pandas) {
    table.schema.metadata ??= new Map();
    table.schema.metadata.set("pandas", JSON.stringify(options.pandas));
  }
  return ArrowAsAnnDataFrameStore.fromIPC(tableToIPC(table, { format: "file" })!);
}

describe("arrow edge cases", () => {
  const ids = ["r0", "r1", "r2", "r3", "r4", "r5"];

  test("dictionary column with nulls: missing values have code -1", async () => {
    const store = arrowStore(
      { id: ids, label: ["b", null, "a", "b", null, "c"] },
      { id: utf8(), label: dictionary(utf8()) },
    );
    expect(await readAll(store, "label/categories")).toEqual(["a", "b", "c"]);
    expect(await readAll(store, "label/codes")).toEqual([1, -1, 0, 1, -1, 2]);
  });

  test("non-dictionary categorical (pandas metadata) with nulls: missing values have code -1", async () => {
    const store = arrowStore(
      { id: ids, label: ["y", "x", null, "x", "y", null] },
      { id: utf8(), label: utf8() },
      { pandas: { index_columns: ["id"], columns: [{ name: "label", pandas_type: "categorical" }] } },
    );
    expect(await readAll(store, "label/categories")).toEqual(["x", "y"]);
    expect(await readAll(store, "label/codes")).toEqual([1, 0, -1, 0, 1, -1]);
  });

  test("multiple batches: categories cover values from every batch", async () => {
    const store = arrowStore(
      { id: ids, label: ["c", "c", "b", "a", "b", "a"] },
      { id: utf8(), label: dictionary(utf8()) },
      { maxBatchRows: 2 },
    );
    expect(await readAll(store, "label/categories")).toEqual(["a", "b", "c"]);
    expect(await readAll(store, "label/codes")).toEqual([2, 2, 1, 0, 1, 0]);
  });

  test("unsigned dictionary index: zero-copy codes keep the uint8 data type", async () => {
    const store = arrowStore(
      { id: ids, label: ["a", "b", "c", "a", "b", "c"] },
      { id: utf8(), label: dictionary(utf8(), uint8()) },
    );
    const meta = await getJson(store, "/label/codes/zarr.json");
    expect(meta.data_type).toBe("uint8");
    expect(await readAll(store, "label/codes")).toEqual([0, 1, 2, 0, 1, 2]);
  });

  test("half-precision float column is widened to float32", async () => {
    const values = [0.5, 1.5, -2, 1024, 0.25, 3];
    const store = arrowStore({ id: ids, h: values, n: [1, 2, 3, 4, 5, 6] }, { id: utf8(), h: float16(), n: int32() });
    const arr = await open(root(store).resolve("h"), { kind: "array" });
    expect(arr.dtype).toBe("float32");
    expect(await readAll(store, "h")).toEqual(values);
  });
});
