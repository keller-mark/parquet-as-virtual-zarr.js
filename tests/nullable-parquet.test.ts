/**
 * Regression tests for nullable (OPTIONAL) columns in parquet files.
 *
 * athletes.parquet contains nullable numeric columns (height: DOUBLE, weight: INT64,
 * date_of_birth: INT32). Nullable floats are plain arrays with NaN for nulls, as in
 * AnnData. Per the AnnData on-disk format spec, each other nullable column is
 * exposed as a Zarr group with `encoding-type: "nullable-integer"` (or
 * `"nullable-string-array"` for strings) containing two arrays:
 *   - values: the data array (NaN / 0 at null positions)
 *   - mask:   uint8 boolean array (1 = null, 0 = present)
 */

import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, test, expect, beforeAll } from "vitest";
// @ts-ignore
import { asyncBufferFromFile, parquetReadObjects, parquetMetadataAsync } from "hyparquet";
import FileSystemStore from "@zarrita/storage/fs";
import { open, get, root } from "zarrita";
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ATHLETES_PATH = resolve(__dirname, "../fixtures/athletes.parquet");

let store: ParquetAsAnnDataFrameStore;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let asyncBuf: any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquetMeta: any;

beforeAll(async () => {
  store = ParquetAsAnnDataFrameStore.fromStore(new FileSystemStore(ATHLETES_PATH));
  asyncBuf = await asyncBufferFromFile(ATHLETES_PATH);
  parquetMeta = await parquetMetadataAsync(asyncBuf);
});

// ── nullable float64 column (height) ────────────────────────────────────────
// AnnData stores float columns with missing values as plain float arrays with NaN,
// so OPTIONAL FLOAT/DOUBLE columns are not exposed as nullable groups.

describe("nullable float64 column (height) — plain array", () => {
  test("column node is a float64 array with encoding-type array", async () => {
    const arr = await open(root(store).resolve("height"), { kind: "array" });
    expect(arr.dtype).toBe("float64");
    expect(arr.attrs["encoding-type"]).toBe("array");
  });

  test("values are NaN exactly at parquet null positions", async () => {
    const arr = await open(root(store).resolve("height"), { kind: "array" });
    const values = Array.from((await get(arr)).data as Float64Array);
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["height"],
    })) as Record<string, number | null>[];
    expect(values.length).toBe(rows.length);
    expect(rows.some((r) => r.height === null)).toBe(true);
    for (let i = 0; i < rows.length; i++) {
      if (rows[i].height === null) expect(values[i]).toBeNaN();
      else expect(values[i]).toBe(rows[i].height);
    }
  });
});

// ── nullable int64 column (weight) ──────────────────────────────────────────

describe("nullable int64 column (weight) — group structure", () => {
  test("column node is a group with encoding-type nullable-integer", async () => {
    const grp = await open(root(store).resolve("weight"), { kind: "group" });
    expect(grp.attrs["encoding-type"]).toBe("nullable-integer");
    expect(grp.attrs["encoding-version"]).toBe("0.1.0");
  });

  test("mask matches parquet null positions and values match elsewhere", async () => {
    const valuesArr = await open(root(store).resolve("weight/values"), { kind: "array" });
    const maskArr = await open(root(store).resolve("weight/mask"), { kind: "array" });
    const values = Array.from((await get(valuesArr)).data as BigInt64Array);
    const mask = Array.from((await get(maskArr)).data as Iterable<number>);

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["weight"],
    })) as Record<string, bigint | null>[];

    expect(mask.length).toBe(rows.length);
    expect(rows.some((r) => r.weight === null)).toBe(true);
    for (let i = 0; i < rows.length; i++) {
      if (rows[i].weight === null) {
        expect(mask[i]).toBeTruthy();
      } else {
        expect(mask[i]).toBeFalsy();
        expect(values[i]).toBe(BigInt(rows[i].weight!));
      }
    }
  });
});

// ── nullable int32 column (date_of_birth) ────────────────────────────────────

describe("nullable int32 column (date_of_birth) — group structure", () => {
  test("column node is a group with encoding-type nullable-integer", async () => {
    const grp = await open(root(store).resolve("date_of_birth"), { kind: "group" });
    expect(grp.attrs["encoding-type"]).toBe("nullable-integer");
  });

  test("values and mask sub-arrays have matching lengths", async () => {
    const valuesArr = await open(root(store).resolve("date_of_birth/values"), { kind: "array" });
    const maskArr = await open(root(store).resolve("date_of_birth/mask"), { kind: "array" });
    const values = Array.from((await get(valuesArr)).data as Iterable<number>);
    const mask = Array.from((await get(maskArr)).data as Iterable<number>);
    expect(values.length).toBe(mask.length);
  });
});
