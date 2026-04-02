import { tableFromIPC, Type, Precision } from "@uwdata/flechette";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";

/**
 * Virtual Zarr v3 store that exposes an Arrow IPC table as an AnnData-Zarr
 * dataframe.
 *
 * Implements zarrita's AsyncReadable interface. The constructor accepts raw
 * Arrow IPC bytes (file or stream format). Arrow record batches map to Zarr
 * chunks, similar to how Parquet row groups map to chunks in
 * ParquetAsAnnDataFrameStore.
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
export class ArrowAsAnnDataFrameStore implements AsyncReadable {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly #table: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #pandasMeta: any = null;
  #categoriesCache = new Map<string, string[]>();
  /**
   * Dictionary-encoded columns whose dictionary values are already sorted
   * (ascending, no duplicates) and shared across all batches.
   * For these columns, Arrow index buffers can be passed through directly
   * as Zarr codes without remapping.
   */
  #zeroCopyCodeCols = new Set<string>();

  constructor(ipcBytes: Uint8Array | ArrayBuffer) {
    const bytes = ipcBytes instanceof ArrayBuffer ? new Uint8Array(ipcBytes) : ipcBytes;
    this.#table = tableFromIPC(bytes);

    const pandasJson = this.#table.schema.metadata?.get("pandas");
    if (pandasJson) {
      this.#pandasMeta = JSON.parse(pandasJson);
    }

    this.#detectZeroCopy();
  }

  static fromIPC(ipcBytes: Uint8Array | ArrayBuffer): ArrowAsAnnDataFrameStore {
    return new ArrowAsAnnDataFrameStore(ipcBytes);
  }

  /**
   * Detect dictionary-encoded columns that can use zero-copy code pass-through.
   * Eligible when: all batches share the same dictionary AND dictionary values
   * are strictly sorted ascending (no duplicates).
   */
  #detectZeroCopy(): void {
    const catCols = this.#categoricalColumnNames();
    for (const name of catCols) {
      const col = this.#getColumn(name);
      if (col.type.typeId !== Type.Dictionary) continue;
      if (col.data.length === 0) continue;

      const dict0 = col.data[0].dictionary;
      if (!dict0 || dict0.length === 0) continue;

      // All batches must share the same dictionary reference
      let eligible = true;
      for (let b = 1; b < col.data.length; b++) {
        if (col.data[b].dictionary !== dict0) { eligible = false; break; }
      }
      if (!eligible) continue;

      // Dictionary values must be strictly sorted ascending (implies no duplicates)
      let sorted = true;
      for (let i = 1; i < dict0.length; i++) {
        if (dict0.at(i) <= dict0.at(i - 1)) { sorted = false; break; }
      }
      if (!sorted) continue;

      this.#zeroCopyCodeCols.add(name);
    }
  }

  #columnNames(): string[] {
    return this.#table.names as string[];
  }

  #indexColumnName(): string {
    return this.#pandasMeta?.index_columns?.[0] ?? this.#columnNames()[0];
  }

  #categoricalColumnNames(): Set<string> {
    // Check pandas metadata first
    const fromPandas: string[] =
      this.#pandasMeta?.columns
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ?.filter((c: any) => c.pandas_type === "categorical")
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .map((c: any) => c.name) ?? [];
    if (fromPandas.length > 0) return new Set(fromPandas);

    // Fall back to detecting Arrow dictionary-encoded columns
    const cats: string[] = [];
    for (const name of this.#columnNames()) {
      const col = this.#table.getChild(name);
      if (col.type.typeId === Type.Dictionary) {
        cats.push(name);
      }
    }
    return new Set(cats);
  }

  #dataColumnOrder(): string[] {
    const indexCol = this.#indexColumnName();
    return this.#columnNames().filter((c) => c !== indexCol);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #getColumn(name: string): any {
    return this.#table.getChild(name);
  }

  #totalRows(): number {
    return this.#table.numRows;
  }

  /** Number of Arrow record batches (used as Zarr chunks). */
  #numBatches(): number {
    return this.#table.children[0].data.length;
  }

  #rowsInBatch(batchIndex: number): number {
    return this.#table.children[0].data[batchIndex].length;
  }

  // ── Arrow type → Zarr type mapping ──────────────────────────────────────

  #arrowTypeToDataType(colName: string): string {
    const col = this.#getColumn(colName);
    const type = col.type;
    switch (type.typeId) {
      case Type.Int:
        if (type.bitWidth === 8) return type.signed ? "int8" : "uint8";
        if (type.bitWidth === 16) return type.signed ? "int16" : "uint16";
        if (type.bitWidth === 64) return type.signed ? "int64" : "uint64";
        return type.signed ? "int32" : "uint32";
      case Type.Float:
        if (type.precision === Precision.DOUBLE) return "float64";
        return "float32";
      default:
        return "string";
    }
  }

  #arrowTypeToDtype(colName: string): string {
    const col = this.#getColumn(colName);
    const type = col.type;
    switch (type.typeId) {
      case Type.Int:
        if (type.bitWidth === 8) return type.signed ? "|i1" : "|u1";
        if (type.bitWidth === 16) return type.signed ? "<i2" : "<u2";
        if (type.bitWidth === 64) return type.signed ? "<i8" : "<u8";
        return type.signed ? "<i4" : "<u4";
      case Type.Float:
        if (type.precision === Precision.DOUBLE) return "<f8";
        return "<f4";
      default:
        return "|O";
    }
  }

  #isStringType(colName: string): boolean {
    const dtype = this.#arrowTypeToDtype(colName);
    return dtype === "|O";
  }

  // ── Data extraction ─────────────────────────────────────────────────────

  /**
   * Read numeric column data for a specific batch as raw bytes.
   */
  #readNumericBatch(colName: string, batchIndex: number): Uint8Array {
    const col = this.#getColumn(colName);
    const batch = col.data[batchIndex];
    const values = batch.values;
    return new Uint8Array(values.buffer, values.byteOffset, batch.length * values.BYTES_PER_ELEMENT);
  }

  /**
   * Read string column data for a specific batch.
   */
  #readStringBatch(colName: string, batchIndex: number): string[] {
    const col = this.#getColumn(colName);
    const batch = col.data[batchIndex];
    const strings: string[] = [];
    for (let i = 0; i < batch.length; i++) {
      strings.push(batch.at(i) ?? "");
    }
    return strings;
  }

  /**
   * Get sorted unique categories for a dictionary/categorical column.
   */
  #getCategories(colName: string): string[] {
    if (this.#categoriesCache.has(colName)) {
      return this.#categoriesCache.get(colName)!;
    }
    const col = this.#getColumn(colName);
    if (col.type.typeId === Type.Dictionary) {
      const dict = col.data[0].dictionary;
      if (this.#zeroCopyCodeCols.has(colName)) {
        // Dictionary is already sorted with no duplicates — use directly
        const categories: string[] = [];
        for (let i = 0; i < dict.length; i++) {
          categories.push(dict.at(i));
        }
        this.#categoriesCache.set(colName, categories);
        return categories;
      }
      // Extract dictionary values, deduplicate, and sort
      const values: string[] = [];
      for (let i = 0; i < dict.length; i++) {
        values.push(dict.at(i));
      }
      const categories = [...new Set(values)].sort();
      this.#categoriesCache.set(colName, categories);
      return categories;
    }
    // Non-dictionary categorical: read all values and compute unique sorted set
    const allValues: string[] = [];
    for (let b = 0; b < this.#numBatches(); b++) {
      allValues.push(...this.#readStringBatch(colName, b));
    }
    const categories = [...new Set(allValues)].sort();
    this.#categoriesCache.set(colName, categories);
    return categories;
  }

  /**
   * Read integer codes for a dictionary column batch, remapped to sorted categories.
   */
  #readCodesBatch(colName: string, batchIndex: number): number[] {
    const col = this.#getColumn(colName);
    const batch = col.data[batchIndex];
    const categories = this.#getCategories(colName);
    const catMap = new Map(categories.map((c, i) => [c, i]));

    if (col.type.typeId === Type.Dictionary) {
      // Remap Arrow dictionary indices to sorted category indices
      const dict = batch.dictionary;
      const codes: number[] = [];
      for (let i = 0; i < batch.length; i++) {
        const key = batch.values[i];
        const value = dict.at(key);
        codes.push(catMap.get(value) ?? 0);
      }
      return codes;
    }
    // Non-dictionary: read strings and map to category indices
    const strings = this.#readStringBatch(colName, batchIndex);
    return strings.map((v) => catMap.get(v) ?? 0);
  }

  // ── Encoding helpers ────────────────────────────────────────────────────

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
    const encoded = strings.map((s) => encoder.encode(s ?? ""));
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

  #codeDataType(colName: string, numCategories: number): string {
    if (this.#zeroCopyCodeCols.has(colName)) {
      const bpe = this.#getColumn(colName).data[0].values.BYTES_PER_ELEMENT;
      if (bpe === 1) return "int8";
      if (bpe === 2) return "int16";
      return "int32";
    }
    if (numCategories <= 128) return "int8";
    if (numCategories <= 32768) return "int16";
    return "int32";
  }

  #codeDtype(colName: string, numCategories: number): string {
    if (this.#zeroCopyCodeCols.has(colName)) {
      const bpe = this.#getColumn(colName).data[0].values.BYTES_PER_ELEMENT;
      if (bpe === 1) return "|i1";
      if (bpe === 2) return "<i2";
      return "<i4";
    }
    if (numCategories <= 128) return "|i1";
    if (numCategories <= 32768) return "<i2";
    return "<i4";
  }

  #numericCodecs(dataType: string): unknown[] {
    if (dataType === "int8") return [{ name: "bytes" }];
    return [{ name: "bytes", configuration: { endian: "little" } }];
  }

  #stringCodecs(): unknown[] {
    return [{ name: "vlen-utf8" }];
  }

  // ── Zarr v3 metadata helpers ────────────────────────────────────────────

  #rootGroupMeta(): Record<string, unknown> {
    return {
      zarr_format: 3,
      node_type: "group",
      attributes: {
        "column-order": this.#dataColumnOrder(),
        _index: this.#indexColumnName(),
        "encoding-type": "dataframe",
        "encoding-version": "0.2.0",
      },
    };
  }

  #numericArrayMeta(colName: string): Record<string, unknown> {
    const dataType = this.#arrowTypeToDataType(colName);
    const numRows = this.#totalRows();
    const chunkSize = this.#rowsInBatch(0);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: dataType,
      chunk_grid: { name: "regular", configuration: { chunk_shape: [chunkSize] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: 0,
      codecs: this.#numericCodecs(dataType),
      attributes: { "encoding-type": "array", "encoding-version": "0.2.0" },
    };
  }

  #stringArrayMeta(encodingType: string): Record<string, unknown> {
    const numRows = this.#totalRows();
    const chunkSize = this.#rowsInBatch(0);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: "string",
      chunk_grid: { name: "regular", configuration: { chunk_shape: [chunkSize] } },
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

  #codesArrayMeta(colName: string): Record<string, unknown> {
    const categories = this.#getCategories(colName);
    const dataType = this.#codeDataType(colName, categories.length);
    const numRows = this.#totalRows();
    const chunkSize = this.#rowsInBatch(0);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: dataType,
      chunk_grid: { name: "regular", configuration: { chunk_shape: [chunkSize] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: 0,
      codecs: this.#numericCodecs(dataType),
      attributes: {},
    };
  }

  #categoriesArrayMeta(colName: string): Record<string, unknown> {
    const categories = this.#getCategories(colName);
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

  // ── Consolidated metadata ───────────────────────────────────────────────

  async getConsolidatedMetadata(): Promise<Record<string, unknown>> {
    const catCols = this.#categoricalColumnNames();
    const allCols = this.#columnNames();
    const metadata: Record<string, unknown> = {};

    for (const col of allCols) {
      if (catCols.has(col)) {
        metadata[col] = {
          ...this.#categoricalGroupMeta(),
          consolidated_metadata: { kind: "inline", must_understand: false, metadata: {} },
        };
        metadata[`${col}/codes`] = this.#codesArrayMeta(col);
        metadata[`${col}/categories`] = this.#categoriesArrayMeta(col);
      } else {
        const isIndex = col === this.#indexColumnName();
        if (isIndex || this.#isStringType(col)) {
          metadata[col] = this.#stringArrayMeta("string-array");
        } else {
          metadata[col] = this.#numericArrayMeta(col);
        }
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

  // ── AsyncReadable interface ─────────────────────────────────────────────

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
    const allCols = new Set(this.#columnNames());
    if (!allCols.has(colName)) return undefined;

    const catCols = this.#categoricalColumnNames();
    const isCat = catCols.has(colName);
    const isIndex = colName === this.#indexColumnName();
    const numBatches = this.#numBatches();

    // /{col}/zarr.json
    if (parts.length === 2 && parts[1] === "zarr.json") {
      if (isCat) return this.#json(this.#categoricalGroupMeta());
      if (isIndex || this.#isStringType(colName)) return this.#json(this.#stringArrayMeta("string-array"));
      return this.#json(this.#numericArrayMeta(colName));
    }

    // /{col}/c/{chunk}
    if (parts.length === 3 && parts[1] === "c" && !isCat) {
      const chunkIndex = Number(parts[2]);
      if (Number.isInteger(chunkIndex) && chunkIndex >= 0 && chunkIndex < numBatches) {
        if (isIndex || this.#isStringType(colName)) {
          return this.#encodeVlenUtf8(this.#readStringBatch(colName, chunkIndex));
        }
        return this.#readNumericBatch(colName, chunkIndex);
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
          if (Number.isInteger(chunkIndex) && chunkIndex >= 0 && chunkIndex < numBatches) {
            if (this.#zeroCopyCodeCols.has(colName)) {
              // Zero-copy: return raw Arrow index buffer directly
              const col = this.#getColumn(colName);
              const batch = col.data[chunkIndex];
              const values = batch.values;
              return new Uint8Array(values.buffer, values.byteOffset, batch.length * values.BYTES_PER_ELEMENT);
            }
            const codes = this.#readCodesBatch(colName, chunkIndex);
            const categories = this.#getCategories(colName);
            const codeDtype = this.#codeDtype(colName, categories.length);
            return this.#encodeTypedArray(codes, codeDtype);
          }
        }
      }

      if (subGroup === "categories") {
        if (parts.length === 3 && parts[2] === "zarr.json") {
          return this.#json(this.#categoriesArrayMeta(colName));
        }
        if (parts.length === 4 && parts[2] === "c") {
          const chunkIndex = Number(parts[3]);
          if (Number.isInteger(chunkIndex) && chunkIndex === 0) {
            const categories = this.#getCategories(colName);
            return this.#encodeVlenUtf8(categories);
          }
        }
      }
    }

    return undefined;
  }
}
