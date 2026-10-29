/**
 * Compatibility tests with anndata.js.
 *
 * A ParquetAsAnnDataFrameStore is mounted as the `obs` dataframe of a virtual
 * AnnData-Zarr store and read with anndata.js's `readZarr`. Each column is then
 * read with anndata.js's `get` and compared against the same column read by
 * anndata.js from the ground-truth AnnData-Zarr store (or from hyparquet when
 * there is no Zarr equivalent).
 */

import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import { describe, test, expect } from "vitest";
// @ts-ignore - hyparquet is a JS package
import { asyncBufferFromFile, parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import FileSystemStore from "@zarrita/storage/fs";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";
import { slice } from "zarrita";
import { readZarr, get, isLazyCategoricalArray, isLazyNullableArray, LazyCategoricalArray } from "anndata.js";
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "../fixtures/output");
const ATHLETES_PATH = resolve(__dirname, "../fixtures/athletes.parquet");

// ── helpers ────────────────────────────────────────────────────────────────

const ROOT_GROUP = new TextEncoder().encode(
  JSON.stringify({ zarr_format: 3, node_type: "group", attributes: {} }),
);

/**
 * Expose a dataframe store as `/obs` of an otherwise-empty AnnData-Zarr root,
 * so that it can be opened with anndata.js's `readZarr`.
 */
function mountAsObs(df: AsyncReadable): AsyncReadable {
  const inner = (key: AbsolutePath): AbsolutePath | undefined => {
    if (key === "/obs") return "/";
    if (key.startsWith("/obs/")) return key.slice("/obs".length) as AbsolutePath;
    return undefined;
  };
  return {
    async get(key: AbsolutePath) {
      if (key === "/zarr.json") return ROOT_GROUP;
      const k = inner(key);
      return k === undefined ? undefined : df.get(k);
    },
    async getRange(key: AbsolutePath, range: RangeQuery) {
      const k = inner(key);
      return k === undefined ? undefined : df.getRange!(k, range);
    },
  };
}

function parquetObs(path: string) {
  return readZarr(mountAsObs(ParquetAsAnnDataFrameStore.fromStore(new FileSystemStore(path))));
}

function zarrObs(path: string) {
  return readZarr(new FileSystemStore(path));
}

/** Normalize a chunk returned by anndata.js `get` into a plain array. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function values(chunk: any): unknown[] {
  return Array.from(chunk.data as Iterable<unknown>);
}

// ── parquet vs AnnData-Zarr ground truth ───────────────────────────────────

const CASES = [
  { name: "obs.parquet", parquet: "obs.parquet", zarr: "adata.zarr" },
  { name: "obs_multipart", parquet: "obs_multipart", zarr: "adata.zarr" },
  { name: "obs_edge.parquet", parquet: "obs_edge.parquet", zarr: "adata_edge.zarr" },
  { name: "obs_plain_snappy.parquet", parquet: "obs_plain_snappy.parquet", zarr: "adata_plain.zarr" },
];

describe.each(CASES)("anndata.js reads $name like $zarr", ({ parquet, zarr }) => {
  const virtual = parquetObs(resolve(OUT, parquet));
  const expected = zarrObs(resolve(OUT, zarr));

  async function columns(): Promise<string[]> {
    const grp = await (await expected).obs!.axisRoot();
    return grp.attrs["column-order"] as string[];
  }

  test("readZarr finds obs and no other axes", async () => {
    const adata = await virtual;
    expect(adata.obs).toBeDefined();
    expect(adata.var).toBeUndefined();
    expect(adata.X).toBeUndefined();
  });

  test("obs attributes match", async () => {
    const v = await (await virtual).obs!.axisRoot();
    const e = await (await expected).obs!.axisRoot();
    expect(v.attrs).toEqual(e.attrs);
  });

  test("obsNames match", async () => {
    const v = await (await virtual).obsNames();
    const e = await (await expected).obsNames();
    expect(values(await get(v, [null]))).toEqual(values(await get(e, [null])));
  });

  test("every column has the same element type", async () => {
    const v = (await virtual).obs!;
    const e = (await expected).obs!;
    for (const col of await columns()) {
      expect(await v.has(col)).toBe(true);
      const [vElem, eElem] = await Promise.all([v.get(col), e.get(col)]);
      expect(isLazyCategoricalArray(vElem), col).toBe(isLazyCategoricalArray(eElem));
      if (isLazyCategoricalArray(vElem)) continue;
      // OPTIONAL integer parquet columns are always nullable-integer (SPEC.md §3.5),
      // even when the equivalent AnnData column is a plain array without missing values.
      if (isLazyNullableArray(vElem) && !isLazyNullableArray(eElem)) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        expect(vElem.values.dtype, col).toBe((eElem as any).dtype);
        continue;
      }
      expect(isLazyNullableArray(vElem), col).toBe(isLazyNullableArray(eElem));
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((vElem as any).dtype, col).toBe((eElem as any).dtype);
    }
  });

  test("full column reads match", async () => {
    const v = (await virtual).obs!;
    const e = (await expected).obs!;
    for (const col of await columns()) {
      const [vElem, eElem] = await Promise.all([v.get(col), e.get(col)]);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const [vData, eData] = await Promise.all([get(vElem as any, [null]), get(eElem as any, [null])]);
      expect(values(vData), col).toEqual(values(eData));
    }
  });

  test("sliced column reads match", async () => {
    const v = (await virtual).obs!;
    const e = (await expected).obs!;
    for (const col of await columns()) {
      const [vElem, eElem] = await Promise.all([v.get(col), e.get(col)]);
      for (const sel of [slice(0, 7), slice(13, 58), slice(90, null)]) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const [vData, eData] = await Promise.all([get(vElem as any, [sel]), get(eElem as any, [sel])]);
        expect(values(vData), `${col}[${sel.start}:${sel.stop}]`).toEqual(values(eData));
      }
    }
  });

  test("scalar reads match", async () => {
    const v = (await virtual).obs!;
    const e = (await expected).obs!;
    for (const col of await columns()) {
      const [vElem, eElem] = await Promise.all([v.get(col), e.get(col)]);
      for (const i of [0, 42, 99]) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const [vVal, eVal] = await Promise.all([get(vElem as any, [i]), get(eElem as any, [i])]);
        expect(vVal, `${col}[${i}]`).toEqual(eVal);
      }
    }
  });

  test("categorical columns expose codes and categories", async () => {
    const v = (await virtual).obs!;
    const e = (await expected).obs!;
    for (const col of await columns()) {
      const [vElem, eElem] = await Promise.all([v.get(col), e.get(col)]);
      if (!isLazyCategoricalArray(eElem)) continue;
      expect(vElem).toBeInstanceOf(LazyCategoricalArray);
      const vCat = vElem as LazyCategoricalArray<never, never, never>;
      const eCat = eElem as LazyCategoricalArray<never, never, never>;
      expect(values(await get(vCat.categories, [null]))).toEqual(values(await get(eCat.categories, [null])));
      expect(values(await get(vCat.codes, [null])).map(Number)).toEqual(
        values(await get(eCat.codes, [null])).map(Number),
      );
    }
  });

  test("missing column is reported as absent", async () => {
    const v = (await virtual).obs!;
    expect(await v.has("not_a_column")).toBe(false);
    await expect(v.get("not_a_column")).rejects.toThrow();
  });
});

// ── nullable columns vs hyparquet ground truth ─────────────────────────────

describe("anndata.js reads nullable columns (athletes.parquet)", async () => {
  const virtual = parquetObs(ATHLETES_PATH);
  const file = await asyncBufferFromFile(ATHLETES_PATH);
  const metadata = await parquetMetadataAsync(file);
  const rows = (await parquetReadObjects({ file, metadata })) as Record<string, unknown>[];

  async function obs() {
    return (await virtual).obs!;
  }

  test("nullable integer column is a LazyNullableArray whose nulls match parquet", async () => {
    const elem = await (await obs()).get("weight");
    expect(isLazyNullableArray(elem)).toBe(true);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const data = values(await get(elem as any, [null]));
    expect(data.length).toBe(rows.length);
    expect(data.some((d) => d === null)).toBe(true);
    for (let i = 0; i < rows.length; i++) {
      const expected = rows[i].weight;
      if (expected === null) expect(data[i]).toBeNull();
      else expect(Number(data[i])).toBe(Number(expected));
    }
  });

  test("nullable float column is a plain array with NaN for nulls", async () => {
    const elem = await (await obs()).get("height");
    expect(isLazyNullableArray(elem)).toBe(false);
    expect(isLazyCategoricalArray(elem)).toBe(false);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const data = values(await get(elem as any, [null])) as number[];
    for (let i = 0; i < rows.length; i++) {
      if (rows[i].height === null) expect(data[i]).toBeNaN();
      else expect(data[i]).toBe(rows[i].height);
    }
  });

  test("scalar reads of nullable columns return null at missing positions", async () => {
    const elem = await (await obs()).get("weight");
    const missing = rows.findIndex((r) => r.weight === null);
    const present = rows.findIndex((r) => r.weight !== null);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(await get(elem as any, [missing])).toBeNull();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect(Number(await get(elem as any, [present]))).toBe(Number(rows[present].weight));
  });
});
