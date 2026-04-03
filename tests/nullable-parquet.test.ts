/**
 * Regression tests for nullable (OPTIONAL) columns in parquet files.
 *
 * athletes.parquet contains nullable numeric columns (height: DOUBLE, weight: INT64,
 * date_of_birth: INT32) which previously caused "Invalid typed array length" errors
 * because definition levels were skipped rather than decoded.
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

describe("nullable float64 column (height)", () => {
  test("reading does not throw", async () => {
    const arr = await open(root(store).resolve("height"), { kind: "array" });
    await expect(get(arr)).resolves.toBeDefined();
  });

  test("returns the correct number of rows", async () => {
    const arr = await open(root(store).resolve("height"), { kind: "array" });
    const chunk = await get(arr);
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["height"],
    })) as Record<string, number | null>[];
    expect((chunk.data as Float64Array).length).toBe(rows.length);
  });

  test("null positions are NaN and non-null positions match parquet values", async () => {
    const arr = await open(root(store).resolve("height"), { kind: "array" });
    const chunk = await get(arr);
    const values = chunk.data as Float64Array;

    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["height"],
    })) as Record<string, number | null>[];

    // Verify there are actual nulls in this column (guards against a trivially passing test)
    const nullCount = rows.filter((r) => r.height === null).length;
    expect(nullCount).toBeGreaterThan(0);

    for (let i = 0; i < rows.length; i++) {
      if (rows[i].height === null) {
        expect(Number.isNaN(values[i])).toBe(true);
      } else {
        expect(values[i]).toBe(rows[i].height);
      }
    }
  });
});

describe("nullable int32 column (date_of_birth)", () => {
  test("reading does not throw", async () => {
    const arr = await open(root(store).resolve("date_of_birth"), { kind: "array" });
    await expect(get(arr)).resolves.toBeDefined();
  });

  test("returns the correct number of rows", async () => {
    const arr = await open(root(store).resolve("date_of_birth"), { kind: "array" });
    const chunk = await get(arr);
    const rows = (await parquetReadObjects({
      file: asyncBuf,
      metadata: parquetMeta,
      columns: ["date_of_birth"],
    })) as Record<string, number | null>[];
    expect((chunk.data as Int32Array).length).toBe(rows.length);
  });
});
