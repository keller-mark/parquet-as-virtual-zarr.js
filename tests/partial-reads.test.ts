/**
 * Verify that ParquetAsAnnDataFrameStore reads the minimal number of bytes
 * from its inner AsyncReadable store.
 *
 * Strategy
 * --------
 * A StoreSpy wraps the parquet file bytes in memory and intercepts every
 * getRange() call.  After the one-time init (which reads the full file once
 * via get() to discover byteLength, then reads the parquet footer via one
 * getRange call), every subsequent column-chunk request should add exactly one
 * targeted getRange call whose offset/length matches the column's row-group
 * byte range recorded in the parquet metadata.
 *
 * Reference pattern: https://github.com/keller-mark/hdf5-as-virtual-zarr.js/blob/main/test/partial-reads.test.ts
 */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import { describe, test, expect, beforeAll } from "vitest";
// @ts-ignore - hyparquet is a JS package
import { asyncBufferFromFile, parquetMetadataAsync } from "hyparquet";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";
import { open, root } from "zarrita";
import { ParquetAsAnnDataFrameStore } from "../src/parquet-store.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PARQUET_PATH = resolve(__dirname, "../fixtures/output/obs.parquet");

// ── StoreSpy ───────────────────────────────────────────────────────────────

/**
 * In-memory AsyncReadable that records every get() and getRange() call.
 * get() should never be called by ParquetAsAnnDataFrameStore (it always uses getRange).
 * getRange() slices the in-memory buffer and appends to fetchCalls.
 */
class StoreSpy implements AsyncReadable {
  readonly #buf: ArrayBuffer;
  readonly fileSize: number;
  readonly fetchCalls: Array<{ offset: number; length: number }> = [];
  getCalls = 0;

  constructor(fileBuffer: ArrayBuffer) {
    this.#buf = fileBuffer;
    this.fileSize = fileBuffer.byteLength;
  }

  async get(_key: AbsolutePath): Promise<Uint8Array | undefined> {
    this.getCalls++;
    return new Uint8Array(this.#buf);
  }

  async getRange(
    _key: AbsolutePath,
    range: RangeQuery,
  ): Promise<Uint8Array | undefined> {
    let offset: number;
    let length: number;
    if ("suffixLength" in range) {
      offset = this.fileSize - range.suffixLength;
      length = range.suffixLength;
    } else {
      offset = range.offset;
      length = range.length;
    }
    this.fetchCalls.push({ offset, length });
    return new Uint8Array(this.#buf, offset, length);
  }

  /** Number of unique (non-overlapping) bytes fetched via getRange. */
  uniqueBytesFetched(): number {
    if (this.fetchCalls.length === 0) return 0;
    const intervals = this.fetchCalls
      .map(({ offset, length }) => [offset, offset + length] as [number, number])
      .sort((a, b) => a[0] - b[0]);
    let total = 0;
    let [curStart, curEnd] = intervals[0];
    for (let i = 1; i < intervals.length; i++) {
      const [start, end] = intervals[i];
      if (start <= curEnd) {
        curEnd = Math.max(curEnd, end);
      } else {
        total += curEnd - curStart;
        [curStart, curEnd] = [start, end];
      }
    }
    total += curEnd - curStart;
    return total;
  }

  /** Fraction of the file fetched via getRange (0–1). */
  fractionRead(): number {
    return this.uniqueBytesFetched() / this.fileSize;
  }
}

// ── fixture helpers ────────────────────────────────────────────────────────

function makeStoreSpy(): StoreSpy {
  const raw = readFileSync(PARQUET_PATH);
  const ab = raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength);
  return new StoreSpy(ab as ArrayBuffer);
}

/** Returns {offset, length} for one column in one row group from parquet metadata. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function colByteRange(meta: any, rgIndex: number, colName: string) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const col = meta.row_groups[rgIndex].columns.find(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (c: any) => c.meta_data.path_in_schema[0] === colName,
  );
  const offset = Number(
    col.meta_data.dictionary_page_offset ?? col.meta_data.data_page_offset,
  );
  const length = Number(col.meta_data.total_compressed_size);
  return { offset, length };
}

// ── shared parquet metadata ────────────────────────────────────────────────

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let parquetMeta: any;
let numRgs: number;

beforeAll(async () => {
  const buf = await asyncBufferFromFile(PARQUET_PATH);
  parquetMeta = await parquetMetadataAsync(buf);
  numRgs = parquetMeta.row_groups.length;
});

// ── tests ──────────────────────────────────────────────────────────────────

describe("init phase", () => {
  test("reading /zarr.json makes at least one getRange call (parquet footer)", async () => {
    const spy = makeStoreSpy();
    await store(spy).get("/zarr.json");
    expect(spy.fetchCalls.length).toBeGreaterThan(0);
  });

  test("get() is never called — init uses only getRange", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });
    await s.get("/n_counts/c/0");
    await s.get("/cell_type/categories/c/0");
    expect(spy.getCalls).toBe(0);
  });

  test("init is memoised: pure schema keys make no new getRange calls", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });
    const callsAfterInit = spy.fetchCalls.length;

    // These keys are derived entirely from the already-parsed parquet footer.
    await open(root(s).resolve("n_counts"), { kind: "array" });
    await open(root(s).resolve("cell_type"), { kind: "group" });

    expect(spy.fetchCalls.length).toBe(callsAfterInit);
  });
});

describe("numeric column chunk reads", () => {
  test("each row group read adds exactly one getRange call", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" }); // trigger init

    for (let rg = 0; rg < numRgs; rg++) {
      const before = spy.fetchCalls.length;
      await s.get(`/n_counts/c/${rg}` as AbsolutePath);
      expect(spy.fetchCalls.length - before).toBe(1);
    }
  });

  test("each getRange call targets exactly the column's row-group byte range", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });

    for (let rg = 0; rg < numRgs; rg++) {
      const before = spy.fetchCalls.length;
      await s.get(`/n_counts/c/${rg}` as AbsolutePath);
      const [call] = spy.fetchCalls.slice(before);
      expect(call).toEqual(colByteRange(parquetMeta, rg, "n_counts"));
    }
  });

  test("reading row group 0 does not fetch bytes belonging to row groups 1-3", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });

    const before = spy.fetchCalls.length;
    await s.get("/n_counts/c/0");
    const [call] = spy.fetchCalls.slice(before);

    for (let rg = 1; rg < numRgs; rg++) {
      const other = colByteRange(parquetMeta, rg, "n_counts");
      const callEnd = call.offset + call.length;
      const otherEnd = other.offset + other.length;
      expect(call.offset < otherEnd && other.offset < callEnd).toBe(false);
    }
  });

  test("different columns in the same row group are fetched independently", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });

    const before = spy.fetchCalls.length;
    await s.get("/n_counts/c/0");
    await s.get("/n_genes/c/0");
    const newCalls = spy.fetchCalls.slice(before);

    expect(newCalls.length).toBe(2);
    expect(newCalls[0]).toEqual(colByteRange(parquetMeta, 0, "n_counts"));
    expect(newCalls[1]).toEqual(colByteRange(parquetMeta, 0, "n_genes"));
  });

  test("column chunk reads are a small fraction of the file (<10% each)", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });

    for (let rg = 0; rg < numRgs; rg++) {
      const before = spy.fetchCalls.length;
      await s.get(`/n_counts/c/${rg}` as AbsolutePath);
      const [call] = spy.fetchCalls.slice(before);
      expect(call.length / spy.fileSize).toBeLessThan(0.1);
    }
  });
});

describe("string index column chunk reads", () => {
  test("each obs_id row group read adds exactly one targeted getRange call", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });

    for (let rg = 0; rg < numRgs; rg++) {
      const before = spy.fetchCalls.length;
      await s.get(`/obs_id/c/${rg}` as AbsolutePath);
      const [call] = spy.fetchCalls.slice(before);
      expect(spy.fetchCalls.length - before).toBe(1);
      expect(call).toEqual(colByteRange(parquetMeta, rg, "obs_id"));
    }
  });
});

describe("categorical column chunk reads", () => {
  test("categories are fetched once then cached; second access makes no new getRange calls", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });

    await s.get("/cell_type/categories/c/0");
    const callsAfterFirst = spy.fetchCalls.length;

    await s.get("/cell_type/categories/c/0");
    expect(spy.fetchCalls.length).toBe(callsAfterFirst);
  });

  test("category getRange calls are scoped to only the categorical column bytes", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });
    const afterInit = spy.fetchCalls.length;

    await s.get("/cell_type/categories/c/0");
    const newCalls = spy.fetchCalls.slice(afterInit);

    const cellTypeRanges = Array.from({ length: numRgs }, (_, rg) =>
      colByteRange(parquetMeta, rg, "cell_type"),
    );

    for (const call of newCalls) {
      const fitsInSomeRange = cellTypeRanges.some(
        ({ offset, length }) =>
          call.offset >= offset && call.offset + call.length <= offset + length,
      );
      expect(fitsInSomeRange).toBe(true);
    }
  });

  test("category reads do not touch unrelated columns (n_counts)", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });
    const afterInit = spy.fetchCalls.length;

    await s.get("/cell_type/categories/c/0");
    const newCalls = spy.fetchCalls.slice(afterInit);

    for (let rg = 0; rg < numRgs; rg++) {
      const nc = colByteRange(parquetMeta, rg, "n_counts");
      const ncEnd = nc.offset + nc.length;
      for (const call of newCalls) {
        const callEnd = call.offset + call.length;
        const overlaps = call.offset < ncEnd && nc.offset < callEnd;
        expect(overlaps).toBe(false);
      }
    }
  });
});

describe("unique bytes fraction", () => {
  test("reading one numeric column across all row groups fetches only those column bytes", async () => {
    const spy = makeStoreSpy();
    const s = store(spy);
    await open(root(s), { kind: "group" });

    for (let rg = 0; rg < numRgs; rg++) {
      await s.get(`/n_counts/c/${rg}` as AbsolutePath);
    }

    expect(spy.getCalls).toBe(0);

    const totalBytes = spy.fetchCalls.reduce((sum, c) => sum + c.length, 0);

    const raw = readFileSync(PARQUET_PATH);
    const tailView = new DataView(raw.buffer, raw.byteOffset + raw.byteLength - 8, 8);
    const metadataLength = tailView.getUint32(0, true);
    const footerBytes = 8 + (metadataLength + 8);

    const columnBytes = numRgs * colByteRange(parquetMeta, 0, "n_counts").length;
    expect(totalBytes).toBe(footerBytes + columnBytes);
    expect(columnBytes / spy.fileSize).toBeLessThan(0.15);
  });
});

// ── tiny helper to keep tests concise ─────────────────────────────────────

function store(spy: StoreSpy): ParquetAsAnnDataFrameStore {
  return ParquetAsAnnDataFrameStore.fromStore(spy);
}
