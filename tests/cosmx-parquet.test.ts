/**
 * Smoke test for cosmx_io_points_part.0.parquet.
 *
 * The file has one row group (943 272 rows) and a mix of:
 *   - categorical columns   : target, cell_ID  (pandas_type = "categorical")
 *   - nullable numeric cols : x, y, fov, x_global_px, z_raw, y_global_px
 *   - nullable string col   : CellComp
 *   - index column          : __null_dask_index__ (excluded from column-order)
 *
 * The test mimics the demo's renderFirstChunkTable() logic: for each column
 * in column-order, detect the encoding-type and read the first chunk via the
 * appropriate sub-path.
 */

import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, test, expect, beforeAll } from "vitest";
import FileSystemStore from "@zarrita/storage/fs";
import { open, get, root, slice } from "zarrita";
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURE = resolve(__dirname, "../fixtures/cosmx_io_points_part.0.parquet");
const NUM_ROWS = 943272;

let store: ParquetAsAnnDataFrameStore;
let grp: Awaited<ReturnType<typeof open>>;

beforeAll(async () => {
  store = ParquetAsAnnDataFrameStore.fromStore(new FileSystemStore(FIXTURE));
  grp = await open(root(store), { kind: "group" });
});

describe("root group", () => {
  test("column-order excludes the index column", async () => {
    const cols = grp.attrs["column-order"] as string[];
    expect(cols).not.toContain("__null_dask_index__");
    expect(cols.length).toBeGreaterThan(0);
  });
});

describe("categorical columns", () => {
  for (const col of ["target", "cell_ID"]) {
    test(`${col}: encoding-type is categorical`, async () => {
      const colNode = await open(grp.resolve(col));
      expect(colNode.attrs?.["encoding-type"]).toBe("categorical");
    });

    test(`${col}: first-chunk codes and categories are readable`, async () => {
      const codesArr = await open(grp.resolve(`${col}/codes`), { kind: "array" });
      const catsArr = await open(grp.resolve(`${col}/categories`), { kind: "array" });
      const chunkSize = codesArr.chunks[0];
      const codesChunk = await get(codesArr, [slice(0, chunkSize)]);
      const catsChunk = await get(catsArr);
      expect(Array.from(codesChunk.data as Iterable<number>).length).toBe(NUM_ROWS);
      expect((catsChunk.data as string[]).length).toBeGreaterThan(0);
    });
  }
});

describe("nullable numeric columns", () => {
  for (const col of ["x", "y", "fov", "x_global_px", "z_raw", "y_global_px"]) {
    test(`${col}: encoding-type is nullable-integer`, async () => {
      const colNode = await open(grp.resolve(col));
      expect(colNode.attrs?.["encoding-type"]).toBe("nullable-integer");
    });

    test(`${col}: values and mask first chunks have correct length`, async () => {
      const valuesArr = await open(grp.resolve(`${col}/values`), { kind: "array" });
      const maskArr = await open(grp.resolve(`${col}/mask`), { kind: "array" });
      const chunkSize = valuesArr.chunks[0];
      const valuesChunk = await get(valuesArr, [slice(0, chunkSize)]);
      const maskChunk = await get(maskArr, [slice(0, chunkSize)]);
      expect(Array.from(valuesChunk.data as Iterable<number>).length).toBe(NUM_ROWS);
      expect(Array.from(maskChunk.data as Iterable<number>).length).toBe(NUM_ROWS);
    });
  }
});

describe("nullable string column (CellComp)", () => {
  test("encoding-type is nullable-string-array", async () => {
    const colNode = await open(grp.resolve("CellComp"));
    expect(colNode.attrs?.["encoding-type"]).toBe("nullable-string-array");
  });

  test("values and mask first chunks have correct length", async () => {
    const valuesArr = await open(grp.resolve("CellComp/values"), { kind: "array" });
    const maskArr = await open(grp.resolve("CellComp/mask"), { kind: "array" });
    const chunkSize = valuesArr.chunks[0];
    const valuesChunk = await get(valuesArr, [slice(0, chunkSize)]);
    const maskChunk = await get(maskArr, [slice(0, chunkSize)]);
    expect((valuesChunk.data as string[]).length).toBe(NUM_ROWS);
    expect(Array.from(maskChunk.data as Iterable<number>).length).toBe(NUM_ROWS);
  });
});
