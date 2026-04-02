import { dsvFormat } from "d3-dsv";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";

/**
 * Column type for CSV stores. Numeric types are used for data columns;
 * "string" for free-text; "categorical" for columns with a small number
 * of repeated string values (encoded as codes + categories, AnnData style).
 */
export type CsvColumnType =
  | "int8" | "int16" | "int32"
  | "float32" | "float64"
  | "string" | "categorical";

export interface CsvAsAnnDataFrameStoreOptions {
  /** Column separator. Default: "," (use "\t" for TSV). */
  separator?: string;
  /** Override auto-detected types for specific columns. */
  columnTypes?: Record<string, CsvColumnType>;
  /** Column to use as the AnnData obs index. Default: first column. */
  indexColumn?: string;
  /** Number of rows per Zarr chunk. Default: all rows in one chunk. */
  chunkSize?: number;
}

/**
 * Virtual Zarr v3 store that exposes a CSV (or TSV) file as an AnnData-Zarr
 * dataframe.
 *
 * Implements zarrita's AsyncReadable interface. Column types are
 * auto-detected from the data (integer → int32, float → float64, strings
 * with ≤50% unique values and ≤1000 distinct values → categorical, else →
 * string). Types can be overridden per-column via `options.columnTypes`.
 *
 * Key routing (Zarr v3):
 *   /zarr.json                        → dataframe group metadata
 *   /{col}/zarr.json                  → array metadata (non-categorical) or group (categorical)
 *   /{col}/c/{chunk}                  → raw chunk bytes (non-categorical)
 *   /{col}/codes/zarr.json            → codes array metadata
 *   /{col}/codes/c/{chunk}           → codes chunk bytes
 *   /{col}/categories/zarr.json       → categories array metadata
 *   /{col}/categories/c/0            → categories chunk bytes (vlen-utf8)
 */
export class CsvAsAnnDataFrameStore implements AsyncReadable {
  readonly #rows: Record<string, string>[];
  readonly #cols: string[];
  readonly #types: Map<string, CsvColumnType>;
  readonly #indexCol: string;
  readonly #chunkSize: number;
  #categoriesCache = new Map<string, string[]>();

  constructor(csvText: string, options: CsvAsAnnDataFrameStoreOptions = {}) {
    const sep = options.separator ?? ",";
    const parsed = dsvFormat(sep).parse(csvText);
    this.#rows = parsed;
    this.#cols = parsed.columns as string[];
    this.#indexCol = options.indexColumn ?? this.#cols[0];
    this.#chunkSize = Math.max(1, options.chunkSize ?? this.#rows.length);
    this.#types = this.#detectTypes(options.columnTypes ?? {});
  }

  static async fromStore(internalStore: AsyncReadable, options?: CsvAsAnnDataFrameStoreOptions): Promise<CsvAsAnnDataFrameStore> {
    // Load all bytes at the store root, which corresponds to the full CSV or TSV file.
    const internalBytes = await internalStore.get("/");
    if (!internalBytes) throw new Error(`internalStore returned undefined for root key`);
    const csvText = new TextDecoder().decode(internalBytes);
    return new CsvAsAnnDataFrameStore(csvText, options);
  }

  static fromText(csvText: string, options?: CsvAsAnnDataFrameStoreOptions): CsvAsAnnDataFrameStore {
    return new CsvAsAnnDataFrameStore(csvText, options);
  }

  // ── Type detection ───────────────────────────────────────────────────────

  #detectTypes(overrides: Record<string, CsvColumnType>): Map<string, CsvColumnType> {
    const result = new Map<string, CsvColumnType>();
    const numRows = this.#rows.length;

    for (const col of this.#cols) {
      if (overrides[col] !== undefined) {
        result.set(col, overrides[col]);
        continue;
      }

      const values = this.#rows.map(r => r[col] ?? "");
      let allNumeric = true;
      let allInteger = true;

      for (const v of values) {
        if (v === "") continue; // treat empty as 0
        const n = Number(v);
        if (isNaN(n)) { allNumeric = false; break; }
        if (!Number.isInteger(n)) allInteger = false;
      }

      if (allNumeric) {
        result.set(col, allInteger ? "int32" : "float64");
        continue;
      }

      const unique = new Set(values);
      if (numRows > 0 && unique.size / numRows <= 0.5 && unique.size <= 1000) {
        result.set(col, "categorical");
      } else {
        result.set(col, "string");
      }
    }

    return result;
  }

  // ── Column / chunk helpers ───────────────────────────────────────────────

  #numChunks(): number {
    return Math.ceil(this.#rows.length / this.#chunkSize);
  }

  #getChunkValues(col: string, chunkIndex: number): string[] {
    const start = chunkIndex * this.#chunkSize;
    const end = Math.min(start + this.#chunkSize, this.#rows.length);
    return this.#rows.slice(start, end).map(r => r[col] ?? "");
  }

  #getCategories(col: string): string[] {
    if (this.#categoriesCache.has(col)) return this.#categoriesCache.get(col)!;
    const all = this.#rows.map(r => r[col] ?? "");
    const cats = [...new Set(all)].sort();
    this.#categoriesCache.set(col, cats);
    return cats;
  }

  #dataColumnOrder(): string[] {
    return this.#cols.filter(c => c !== this.#indexCol);
  }

  // ── Type mapping helpers ─────────────────────────────────────────────────

  #typeToDataType(t: CsvColumnType): string {
    switch (t) {
      case "int8": return "int8";
      case "int16": return "int16";
      case "int32": return "int32";
      case "float32": return "float32";
      case "float64": return "float64";
      default: return "string";
    }
  }

  #typeToDtype(t: CsvColumnType): string {
    switch (t) {
      case "int8": return "|i1";
      case "int16": return "<i2";
      case "int32": return "<i4";
      case "float32": return "<f4";
      case "float64": return "<f8";
      default: return "|O";
    }
  }

  #codeDataType(numCategories: number): string {
    if (numCategories <= 128) return "int8";
    if (numCategories <= 32768) return "int16";
    return "int32";
  }

  #codeDtype(numCategories: number): string {
    if (numCategories <= 128) return "|i1";
    if (numCategories <= 32768) return "<i2";
    return "<i4";
  }

  // ── Encoding ─────────────────────────────────────────────────────────────

  #encodeTypedArray(data: number[], dtype: string): Uint8Array {
    let buffer: ArrayBuffer;
    if (dtype === "<f4")      buffer = new Float32Array(data).buffer;
    else if (dtype === "<f8") buffer = new Float64Array(data).buffer;
    else if (dtype === "<i4") buffer = new Int32Array(data).buffer;
    else if (dtype === "<i2") buffer = new Int16Array(data).buffer;
    else if (dtype === "|i1") buffer = new Int8Array(data).buffer;
    else                      buffer = new Int32Array(data).buffer;
    return new Uint8Array(buffer);
  }

  #encodeVlenUtf8(strings: (string | null | undefined)[]): Uint8Array {
    const encoder = new TextEncoder();
    const encoded = strings.map(s => encoder.encode(s ?? ""));
    const totalBytes = 4 + encoded.reduce((sum, e) => sum + 4 + e.byteLength, 0);
    const buffer = new ArrayBuffer(totalBytes);
    const view = new DataView(buffer);
    view.setUint32(0, strings.length, true);
    let pos = 4;
    for (const e of encoded) {
      view.setUint32(pos, e.byteLength, true);
      pos += 4;
      new Uint8Array(buffer, pos, e.byteLength).set(e);
      pos += e.byteLength;
    }
    return new Uint8Array(buffer);
  }

  #json(obj: unknown): Uint8Array {
    return new TextEncoder().encode(JSON.stringify(obj));
  }

  #encodeNumericChunk(col: string, chunkIndex: number): Uint8Array {
    const t = this.#types.get(col)!;
    const dtype = this.#typeToDtype(t);
    const nums = this.#getChunkValues(col, chunkIndex).map(v => v === "" ? 0 : Number(v));
    return this.#encodeTypedArray(nums, dtype);
  }

  #encodeCodesChunk(col: string, chunkIndex: number): Uint8Array {
    const categories = this.#getCategories(col);
    const catMap = new Map(categories.map((c, i) => [c, i]));
    const codes = this.#getChunkValues(col, chunkIndex).map(v => catMap.get(v) ?? 0);
    const dtype = this.#codeDtype(categories.length);
    return this.#encodeTypedArray(codes, dtype);
  }

  // ── Zarr v3 metadata ─────────────────────────────────────────────────────

  #numericCodecs(dataType: string): unknown[] {
    if (dataType === "int8") return [{ name: "bytes" }];
    return [{ name: "bytes", configuration: { endian: "little" } }];
  }

  #stringCodecs(): unknown[] {
    return [{ name: "vlen-utf8" }];
  }

  #rootGroupMeta(): Record<string, unknown> {
    return {
      zarr_format: 3,
      node_type: "group",
      attributes: {
        "column-order": this.#dataColumnOrder(),
        _index: this.#indexCol,
        "encoding-type": "dataframe",
        "encoding-version": "0.2.0",
      },
    };
  }

  #numericArrayMeta(col: string): Record<string, unknown> {
    const t = this.#types.get(col)!;
    const dataType = this.#typeToDataType(t);
    const numRows = this.#rows.length;
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: dataType,
      chunk_grid: { name: "regular", configuration: { chunk_shape: [this.#chunkSize] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: 0,
      codecs: this.#numericCodecs(dataType),
      attributes: { "encoding-type": "array", "encoding-version": "0.2.0" },
    };
  }

  #stringArrayMeta(encodingType: string): Record<string, unknown> {
    const numRows = this.#rows.length;
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: "string",
      chunk_grid: { name: "regular", configuration: { chunk_shape: [this.#chunkSize] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: "",
      codecs: this.#stringCodecs(),
      attributes: { "encoding-type": encodingType, "encoding-version": "0.2.0" },
    };
  }

  #categoricalGroupMeta(): Record<string, unknown> {
    return {
      zarr_format: 3,
      node_type: "group",
      attributes: {
        ordered: false,
        "encoding-type": "categorical",
        "encoding-version": "0.2.0",
      },
    };
  }

  #codesArrayMeta(col: string): Record<string, unknown> {
    const categories = this.#getCategories(col);
    const dataType = this.#codeDataType(categories.length);
    const numRows = this.#rows.length;
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: dataType,
      chunk_grid: { name: "regular", configuration: { chunk_shape: [this.#chunkSize] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: 0,
      codecs: this.#numericCodecs(dataType),
      attributes: {},
    };
  }

  #categoriesArrayMeta(col: string): Record<string, unknown> {
    const categories = this.#getCategories(col);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [categories.length],
      data_type: "string",
      chunk_grid: { name: "regular", configuration: { chunk_shape: [categories.length] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: "",
      codecs: this.#stringCodecs(),
      attributes: { "encoding-type": "string-array", "encoding-version": "0.2.0" },
    };
  }

  // ── Consolidated metadata ────────────────────────────────────────────────

  async getConsolidatedMetadata(): Promise<Record<string, unknown>> {
    const metadata: Record<string, unknown> = {};

    for (const col of this.#cols) {
      const t = this.#types.get(col)!;
      if (t === "categorical") {
        metadata[col] = {
          ...this.#categoricalGroupMeta(),
          consolidated_metadata: { kind: "inline", must_understand: false, metadata: {} },
        };
        metadata[`${col}/codes`] = this.#codesArrayMeta(col);
        metadata[`${col}/categories`] = this.#categoriesArrayMeta(col);
      } else if (t === "string" || col === this.#indexCol) {
        metadata[col] = this.#stringArrayMeta("string-array");
      } else {
        metadata[col] = this.#numericArrayMeta(col);
      }
    }

    return {
      ...this.#rootGroupMeta(),
      consolidated_metadata: {
        kind: "inline",
        must_understand: false,
        metadata,
      },
    };
  }

  // ── AsyncReadable interface ──────────────────────────────────────────────

  async get(key: AbsolutePath, _opts?: unknown): Promise<Uint8Array | undefined> {
    return this.#route(key);
  }

  async getRange(
    key: AbsolutePath,
    range: RangeQuery,
    _opts?: unknown,
  ): Promise<Uint8Array | undefined> {
    const data = await this.get(key);
    if (!data) return undefined;
    if ("suffixLength" in range) {
      return data.slice(data.byteLength - range.suffixLength);
    }
    return data.slice(range.offset, range.offset + range.length);
  }

  #route(key: string): Uint8Array | undefined {
    if (key === "/zarr.json") {
      return this.#json(this.#rootGroupMeta());
    }

    const parts = key.split("/").filter(Boolean);
    if (parts.length < 2) return undefined;

    const colName = parts[0];
    const allCols = new Set(this.#cols);
    if (!allCols.has(colName)) return undefined;

    const t = this.#types.get(colName)!;
    const isCat = t === "categorical";
    const isIndex = colName === this.#indexCol;
    const isString = t === "string";
    const numChunks = this.#numChunks();

    // /{col}/zarr.json
    if (parts.length === 2 && parts[1] === "zarr.json") {
      if (isCat) return this.#json(this.#categoricalGroupMeta());
      if (isIndex || isString) return this.#json(this.#stringArrayMeta("string-array"));
      return this.#json(this.#numericArrayMeta(colName));
    }

    // /{col}/c/{chunk} (non-categorical)
    if (parts.length === 3 && parts[1] === "c" && !isCat) {
      const chunkIndex = Number(parts[2]);
      if (Number.isInteger(chunkIndex) && chunkIndex >= 0 && chunkIndex < numChunks) {
        if (isIndex || isString) {
          return this.#encodeVlenUtf8(this.#getChunkValues(colName, chunkIndex));
        }
        return this.#encodeNumericChunk(colName, chunkIndex);
      }
    }

    if (isCat) {
      const subGroup = parts[1];

      if (subGroup === "codes") {
        if (parts.length === 3 && parts[2] === "zarr.json") {
          return this.#json(this.#codesArrayMeta(colName));
        }
        if (parts.length === 4 && parts[2] === "c") {
          const chunkIndex = Number(parts[3]);
          if (Number.isInteger(chunkIndex) && chunkIndex >= 0 && chunkIndex < numChunks) {
            return this.#encodeCodesChunk(colName, chunkIndex);
          }
        }
      }

      if (subGroup === "categories") {
        if (parts.length === 3 && parts[2] === "zarr.json") {
          return this.#json(this.#categoriesArrayMeta(colName));
        }
        if (parts.length === 4 && parts[2] === "c") {
          const chunkIndex = Number(parts[3]);
          if (chunkIndex === 0) {
            return this.#encodeVlenUtf8(this.#getCategories(colName));
          }
        }
      }
    }

    return undefined;
  }
}
