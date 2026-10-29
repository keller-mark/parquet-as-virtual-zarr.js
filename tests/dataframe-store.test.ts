/**
 * Tests for ParquetAsDataFrameStore, which exposes Parquet data following the
 * zarr-convention-dataframe spec (references/zarr-convention-dataframe), with
 * categorical columns following zarr-convention-enum (references/zarr-convention-enum).
 *
 * Data is compared against the AnnData-Zarr ground-truth fixture, which uses
 * the equivalent AnnData layout (named members, `categories` instead of `enum_values`).
 */

import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import { describe, test, expect, beforeAll } from "vitest";
import FileSystemStore from "@zarrita/storage/fs";
import { open, get, root } from "zarrita";
import { ParquetAsDataFrameStore } from "../src/dataframe-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PARQUET_PATH = resolve(__dirname, "../fixtures/output/obs.parquet");
const ZARR_OBS = resolve(__dirname, "../fixtures/output/adata.zarr/obs");
const ATHLETES_PATH = resolve(__dirname, "../fixtures/athletes.parquet");

const DF_UUID = "1f1b36d2-3185-6da8-91f6-9a0823af2d33";
const ENUM_UUID = "c906c423-56ed-413c-9943-7b2ff52d18f2";

let store: ParquetAsDataFrameStore;
let zarrStore: FileSystemStore;

beforeAll(() => {
  store = ParquetAsDataFrameStore.fromStore(new FileSystemStore(PARQUET_PATH));
  zarrStore = new FileSystemStore(ZARR_OBS);
});

/** Read column names the way a convention-aware reader would. */
async function readColumns(s: ParquetAsDataFrameStore): Promise<string[]> {
  const arr = await open(root(s).resolve("columns"), { kind: "array" });
  const chunk = await get(arr);
  return Array.from(chunk.data as Iterable<string>);
}

async function memberFor(s: ParquetAsDataFrameStore, name: string): Promise<string> {
  const columns = await readColumns(s);
  const i = columns.indexOf(name);
  if (i < 0) throw new Error(`column ${name} not found`);
  return `column_${i}`;
}

describe("root /zarr.json", () => {
  test("is a group declaring the dataframe convention", async () => {
    const grp = await open(root(store), { kind: "group" });
    const conventions = grp.attrs.zarr_conventions as { uuid: string; name: string }[];
    expect(conventions.find((c) => c.uuid === DF_UUID)?.name).toBe("df:");
  });

  test("has no AnnData encoding attributes", async () => {
    const grp = await open(root(store), { kind: "group" });
    expect(grp.attrs["encoding-type"]).toBeUndefined();
    expect(grp.attrs["column-order"]).toBeUndefined();
    expect(grp.attrs._index).toBeUndefined();
  });

  test("df:shape is [rows, columns] including the index", async () => {
    const grp = await open(root(store), { kind: "group" });
    const zarrIndex = await open(root(zarrStore).resolve("obs_id"), { kind: "array" });
    const zarrAttrs = (await open(root(zarrStore), { kind: "group" })).attrs;
    const numCols = 1 + (zarrAttrs["column-order"] as string[]).length;
    expect(grp.attrs["df:shape"]).toEqual([zarrIndex.shape[0], numCols]);
  });

  test("df:index points at the AnnData index column", async () => {
    const grp = await open(root(store), { kind: "group" });
    const columns = await readColumns(store);
    const zarrAttrs = (await open(root(zarrStore), { kind: "group" })).attrs;
    expect(columns[grp.attrs["df:index"] as number]).toBe(zarrAttrs._index);
  });
});

describe("columns array", () => {
  test("lists the index followed by column-order", async () => {
    const zarrAttrs = (await open(root(zarrStore), { kind: "group" })).attrs;
    expect(await readColumns(store)).toEqual([
      zarrAttrs._index,
      ...(zarrAttrs["column-order"] as string[]),
    ]);
  });

  test("length matches df:shape[1]", async () => {
    const grp = await open(root(store), { kind: "group" });
    const shape = grp.attrs["df:shape"] as number[];
    expect((await readColumns(store)).length).toBe(shape[1]);
  });
});

describe("column member names", () => {
  test("original column names are not members", async () => {
    expect(await store.get("/obs_id/zarr.json")).toBeUndefined();
    expect(await store.get("/n_genes/zarr.json")).toBeUndefined();
  });

  test("out-of-range and zero-padded column members are absent", async () => {
    expect(await store.get("/column_99/zarr.json")).toBeUndefined();
    expect(await store.get("/column_01/zarr.json")).toBeUndefined();
  });
});

describe("plain array column (index)", () => {
  test("obs_id data matches zarr", async () => {
    const arr = await open(root(store).resolve(await memberFor(store, "obs_id")), { kind: "array" });
    const expected = await open(root(zarrStore).resolve("obs_id"), { kind: "array" });
    expect(arr.dtype).toBe(expected.dtype);
    expect(arr.shape).toEqual(expected.shape);
    expect(Array.from((await get(arr)).data as Iterable<string>)).toEqual(
      Array.from((await get(expected)).data as Iterable<string>),
    );
  });

  test("arrays do not carry AnnData encoding attributes", async () => {
    const arr = await open(root(store).resolve(await memberFor(store, "obs_id")), { kind: "array" });
    expect(arr.attrs["encoding-type"]).toBeUndefined();
  });
});

// n_counts and n_genes are OPTIONAL in obs.parquet, so they are exposed as
// 1d-interpreted nullable groups (values + mask), as in ParquetAsAnnDataFrameStore.
describe("nullable numeric columns", () => {
  for (const name of ["n_counts", "n_genes"]) {
    test(`${name} values match zarr and have length df:shape[0]`, async () => {
      const grp = await open(root(store), { kind: "group" });
      const [numRows] = grp.attrs["df:shape"] as number[];
      const member = await memberFor(store, name);
      const arr = await open(root(store).resolve(`${member}/values`), { kind: "array" });
      const expected = await open(root(zarrStore).resolve(name), { kind: "array" });
      expect(arr.dtype).toBe(expected.dtype);
      expect(arr.shape).toEqual([numRows]);
      expect(Array.from((await get(arr)).data as Iterable<number>)).toEqual(
        Array.from((await get(expected)).data as Iterable<number>),
      );
    });
  }
});

describe("enum (categorical) columns", () => {
  for (const name of ["cell_type", "leiden"]) {
    test(`${name} group declares the enum convention`, async () => {
      const grp = await open(root(store).resolve(await memberFor(store, name)), { kind: "group" });
      const conventions = grp.attrs.zarr_conventions as { uuid: string }[];
      expect(conventions.some((c) => c.uuid === ENUM_UUID)).toBe(true);
      expect(grp.attrs["enum:ordered"]).toBe(false);
      expect(grp.attrs["enum:codes"]).toEqual(["codes"]);
      expect(grp.attrs["encoding-type"]).toBeUndefined();
    });

    test(`${name} enum_values match zarr categories`, async () => {
      const member = await memberFor(store, name);
      const arr = await open(root(store).resolve(`${member}/enum_values`), { kind: "array" });
      const expected = await open(root(zarrStore).resolve(`${name}/categories`), { kind: "array" });
      expect(Array.from((await get(arr)).data as Iterable<string>)).toEqual(
        Array.from((await get(expected)).data as Iterable<string>),
      );
    });

    test(`${name} decoded values match zarr`, async () => {
      const member = await memberFor(store, name);
      const codes = await get(await open(root(store).resolve(`${member}/codes`), { kind: "array" }));
      const values = await get(await open(root(store).resolve(`${member}/enum_values`), { kind: "array" }));
      const zCodes = await get(await open(root(zarrStore).resolve(`${name}/codes`), { kind: "array" }));
      const zCats = await get(await open(root(zarrStore).resolve(`${name}/categories`), { kind: "array" }));
      const decode = (c: Iterable<number>, v: ArrayLike<string>) => Array.from(c, (i) => v[i]);
      expect(decode(codes.data as Iterable<number>, values.data as ArrayLike<string>)).toEqual(
        decode(zCodes.data as Iterable<number>, zCats.data as ArrayLike<string>),
      );
    });

    test(`${name} AnnData 'categories' member is not exposed`, async () => {
      const member = await memberFor(store, name);
      expect(await store.get(`/${member}/categories/zarr.json`)).toBeUndefined();
    });
  }
});

describe("nullable columns (athletes.parquet)", () => {
  let athletes: ParquetAsDataFrameStore;
  beforeAll(() => {
    athletes = ParquetAsDataFrameStore.fromStore(new FileSystemStore(ATHLETES_PATH));
  });

  test("nullable column is a group with values and mask of length df:shape[0]", async () => {
    const grp = await open(root(athletes), { kind: "group" });
    const [numRows] = grp.attrs["df:shape"] as number[];
    const member = await memberFor(athletes, "height");
    await open(root(athletes).resolve(member), { kind: "group" });
    const values = await get(await open(root(athletes).resolve(`${member}/values`), { kind: "array" }));
    const mask = await get(await open(root(athletes).resolve(`${member}/mask`), { kind: "array" }));
    expect(values.data.length).toBe(numRows);
    expect(mask.data.length).toBe(numRows);
  });
});

describe("consolidated metadata", () => {
  test("keys use column_{i} members and enum_values", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const keys = Object.keys((consolidated.consolidated_metadata as any).metadata);
    const member = await memberFor(store, "cell_type");
    expect(keys).toContain("columns");
    expect(keys).toContain(`${member}/codes`);
    expect(keys).toContain(`${member}/enum_values`);
    expect(keys.some((k) => k.includes("cell_type") || k.includes("categories"))).toBe(false);
  });

  test("entries match what get() returns", async () => {
    const consolidated = await store.getConsolidatedMetadata();
    const metadata = (consolidated.consolidated_metadata as any).metadata;
    for (const [key, meta] of Object.entries(metadata) as [string, any][]) {
      const bytes = await store.get(`/${key}/zarr.json`);
      const direct = JSON.parse(new TextDecoder().decode(bytes));
      const { consolidated_metadata: _c, ...rest } = meta;
      expect(direct).toEqual(rest);
    }
  });
});
