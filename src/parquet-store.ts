// @ts-ignore - hyparquet is a JS package; types generated on build
import { parquetMetadata, parquetRead } from "hyparquet";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";

interface PartInfo {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  metadata: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  asyncBuffer: any;
  /** Key used to read this part from the inner store. */
  storeKey: string;
}

interface RgMapping {
  partIndex: number;
  localRgIndex: number;
  /** Row offset of this part's start within the global row space. */
  partRowOffset: number;
}

/**
 * Virtual Zarr v3 store that exposes a Parquet file (or multi-part parquet
 * directory) as an AnnData-Zarr dataframe.
 *
 * Implements zarrita's AsyncReadable interface. Zarr key paths are mapped to
 * byte ranges in the Parquet file via row-group-level chunking.
 *
 * Supports both single-file parquet and multi-part parquet directories
 * (part.0.parquet, part.1.parquet, …). Detection is automatic.
 *
 * Key routing (Zarr v3):
 *   /zarr.json                        → dataframe group metadata
 *   /{col}/zarr.json                  → array metadata (non-categorical) or group (categorical)
 *   /{col}/c/{rg}                     → raw chunk bytes (non-categorical)
 *   /{col}/codes/zarr.json            → codes array metadata
 *   /{col}/codes/c/{rg}              → codes chunk bytes
 *   /{col}/categories/zarr.json       → categories array metadata
 *   /{col}/categories/c/0            → categories chunk bytes (vlen-utf8)
 */
export class ParquetAsAnnDataFrameZarr implements AsyncReadable {
  readonly #store: AsyncReadable;
  #initialized = false;
  /** Per-part parquet metadata and async buffers. */
  #parts: PartInfo[] = [];
  /** Maps global row group index → part and local row group index. */
  #rgMap: RgMapping[] = [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #pandasMeta: any = null;
  #categoriesCache = new Map<string, string[]>();

  constructor(store: AsyncReadable) {
    this.#store = store;
  }

  static fromStore(store: AsyncReadable): ParquetAsAnnDataFrameZarr {
    return new ParquetAsAnnDataFrameZarr(store);
  }

  /**
   * Read the parquet footer for a given store key and return parsed metadata +
   * an AsyncBuffer for column reads. Returns null if the key is not a valid
   * parquet file.
   */
  async #readParquetFooter(storeKey: string): Promise<PartInfo | null> {
    if (!this.#store.getRange) {
      throw new Error("ParquetAsAnnDataFrameZarr: inner store must support getRange");
    }

    // Read last 8 bytes to discover metadata length and validate magic.
    let tail: Uint8Array | undefined;
    try {
      tail = await this.#store.getRange(storeKey as AbsolutePath, { suffixLength: 8 });
    } catch {
      return null;
    }
    if (!tail) return null;
    const tailBuf = tail.buffer.slice(tail.byteOffset, tail.byteOffset + tail.byteLength) as ArrayBuffer;
    const tailView = new DataView(tailBuf);
    // Validate PAR1 magic (last 4 bytes of the file).
    if (tailView.getUint32(4, true) !== 0x31524150) return null;
    const metadataLength = tailView.getUint32(0, true);

    // Fetch exactly the footer bytes (metadata + 8-byte suffix).
    const footerSize = metadataLength + 8;
    const footerBytes = await this.#store.getRange(storeKey as AbsolutePath, { suffixLength: footerSize });
    if (!footerBytes) return null;
    const footerBuf = footerBytes.buffer.slice(
      footerBytes.byteOffset,
      footerBytes.byteOffset + footerBytes.byteLength,
    ) as ArrayBuffer;

    const metadata = parquetMetadata(footerBuf);

    const asyncBuffer = {
      byteLength: Number.MAX_SAFE_INTEGER,
      slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
        if (end === undefined) {
          throw new Error("ParquetAsAnnDataFrameZarr: unbounded slice — this is a bug");
        }
        const length = end - start;
        if (length <= 0) return new ArrayBuffer(0);
        const bytes = await this.#store.getRange!(storeKey as AbsolutePath, { offset: start, length });
        if (!bytes) {
          throw new Error(`ParquetAsAnnDataFrameZarr: getRange returned nothing for key=${storeKey} offset=${start} length=${length}`);
        }
        return bytes.buffer.slice(
          bytes.byteOffset,
          bytes.byteOffset + bytes.byteLength,
        ) as ArrayBuffer;
      },
    };

    return { metadata, asyncBuffer, storeKey };
  }

  /**
   * Lazily initialise: detect single-file vs multi-part mode, read parquet
   * metadata, and build AsyncBuffers for column reads.
   */
  async #init(): Promise<void> {
    if (this.#initialized) return;

    // Try single-file mode first (key "/").
    const singlePart = await this.#readParquetFooter("/");
    if (singlePart) {
      this.#parts = [singlePart];
    } else {
      // Multi-part mode: try part.0.parquet, part.1.parquet, …
      const parts: PartInfo[] = [];
      let partIndex = 0;
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const part = await this.#readParquetFooter(`/part.${partIndex}.parquet`);
        if (!part) break;
        parts.push(part);
        partIndex++;
      }
      if (parts.length === 0) {
        throw new Error("ParquetAsAnnDataFrameZarr: no valid parquet file found (tried '/' and '/part.0.parquet')");
      }
      this.#parts = parts;
    }

    // Build the global row group mapping.
    let globalRowOffset = 0;
    for (let pi = 0; pi < this.#parts.length; pi++) {
      const rgs = this.#parts[pi].metadata.row_groups;
      for (let ri = 0; ri < rgs.length; ri++) {
        this.#rgMap.push({ partIndex: pi, localRgIndex: ri, partRowOffset: globalRowOffset });
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      globalRowOffset += rgs.reduce((sum: number, rg: any) => sum + Number(rg.num_rows), 0);
    }

    // Extract pandas metadata from the first part.
    const firstMeta = this.#parts[0].metadata;
    const pandasKv = firstMeta.key_value_metadata?.find(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (kv: any) => kv.key === "pandas",
    );
    if (pandasKv) {
      this.#pandasMeta = JSON.parse(pandasKv.value);
    }

    this.#initialized = true;
  }

  /** Schema from first part (all parts share the same schema). */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  get #schema(): any[] {
    return this.#parts[0].metadata.schema;
  }

  /** All column names from parquet schema (leaf elements only). */
  #columnNames(): string[] {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#schema.filter((s: any) => s.type).map((s: any) => s.name);
  }

  #indexColumnName(): string {
    return this.#pandasMeta?.index_columns?.[0] ?? this.#columnNames()[0];
  }

  #categoricalColumnNames(): Set<string> {
    const cats: string[] =
      this.#pandasMeta?.columns
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ?.filter((c: any) => c.pandas_type === "categorical")
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .map((c: any) => c.name) ?? [];
    return new Set(cats);
  }

  #dataColumnOrder(): string[] {
    const indexCol = this.#indexColumnName();
    return this.#columnNames().filter((c) => c !== indexCol);
  }

  #parquetTypeToDataType(colName: string): string {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const schema = this.#schema.find((s: any) => s.name === colName);
    if (!schema) return "string";
    switch (schema.type) {
      case "FLOAT":  return "float32";
      case "DOUBLE": return "float64";
      case "INT32":  return "int32";
      case "INT64":  return "int64";
      default:       return "string";
    }
  }

  #parquetTypeToDtype(colName: string): string {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const schema = this.#schema.find((s: any) => s.name === colName);
    if (!schema) return "|O";
    switch (schema.type) {
      case "FLOAT":  return "<f4";
      case "DOUBLE": return "<f8";
      case "INT32":  return "<i4";
      case "INT64":  return "<i8";
      default:       return "|O";
    }
  }

  /** [rowStart, rowEnd) for a given global row group index. */
  #rowGroupRange(rgIndex: number): [number, number] {
    const mapping = this.#rgMap[rgIndex];
    const part = this.#parts[mapping.partIndex];
    let localStart = 0;
    for (let i = 0; i < mapping.localRgIndex; i++) {
      localStart += Number(part.metadata.row_groups[i].num_rows);
    }
    const numRows = Number(part.metadata.row_groups[mapping.localRgIndex].num_rows);
    const globalStart = mapping.partRowOffset + localStart;
    return [globalStart, globalStart + numRows];
  }

  #totalRows(): number {
    let total = 0;
    for (const part of this.#parts) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      total += part.metadata.row_groups.reduce((sum: number, rg: any) => sum + Number(rg.num_rows), 0);
    }
    return total;
  }

  #rowsInGroup(rgIndex: number): number {
    const mapping = this.#rgMap[rgIndex];
    return Number(this.#parts[mapping.partIndex].metadata.row_groups[mapping.localRgIndex].num_rows);
  }

  /** Total number of global row groups across all parts. */
  #numRowGroups(): number {
    return this.#rgMap.length;
  }

  /**
   * Compute the local [rowStart, rowEnd) within a part for a given global row group index.
   */
  #localRowRangeForRg(rgIndex: number): { part: PartInfo; localRowStart: number; localRowEnd: number } {
    const mapping = this.#rgMap[rgIndex];
    const part = this.#parts[mapping.partIndex];
    let localRowStart = 0;
    for (let i = 0; i < mapping.localRgIndex; i++) {
      localRowStart += Number(part.metadata.row_groups[i].num_rows);
    }
    const localRowEnd = localRowStart + Number(part.metadata.row_groups[mapping.localRgIndex].num_rows);
    return { part, localRowStart, localRowEnd };
  }

  /**
   * Read one column from a specific row group via parquetRead onChunk.
   * For numeric columns, returns the raw typed array (e.g. Float32Array).
   * For string columns, returns a plain string array.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async #readColumnChunkForRg(colName: string, rgIndex: number): Promise<any> {
    const { part, localRowStart, localRowEnd } = this.#localRowRangeForRg(rgIndex);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const chunks: any[] = [];
    await parquetRead({
      file: part.asyncBuffer,
      metadata: part.metadata,
      columns: [colName],
      rowStart: localRowStart,
      rowEnd: localRowEnd,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      onChunk(chunk: any) {
        chunks.push(chunk.columnData);
      },
    });
    if (chunks.length === 1) return chunks[0];
    // Multiple pages: concatenate
    if (ArrayBuffer.isView(chunks[0])) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const Ctor = (chunks[0] as any).constructor as { new (len: number): any };
      const totalLen = chunks.reduce((s: number, c: { length: number }) => s + c.length, 0);
      const merged = new Ctor(totalLen);
      let offset = 0;
      for (const c of chunks) {
        merged.set(c, offset);
        offset += c.length;
      }
      return merged;
    }
    return chunks.flat();
  }

  /**
   * Read all values for a column across all parts/row groups.
   * Always returns a flat array of values (for category extraction).
   */
  async #readAllColumnValues(colName: string): Promise<unknown[]> {
    const allValues: unknown[] = [];
    for (let rg = 0; rg < this.#numRowGroups(); rg++) {
      const chunk = await this.#readColumnChunkForRg(colName, rg);
      if (ArrayBuffer.isView(chunk)) {
        allValues.push(...Array.from(chunk as unknown as ArrayLike<unknown>));
      } else {
        allValues.push(...chunk);
      }
    }
    return allValues;
  }

  async #getCategories(colName: string): Promise<string[]> {
    if (this.#categoriesCache.has(colName)) {
      return this.#categoriesCache.get(colName)!;
    }
    const allValues = await this.#readAllColumnValues(colName);
    const categories = [...new Set(allValues as string[])].sort();
    this.#categoriesCache.set(colName, categories);
    return categories;
  }

  #encodeTypedArray(data: number[], dtype: string): Uint8Array {
    let buffer: ArrayBuffer;
    if (dtype === "<f4")     buffer = new Float32Array(data).buffer;
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

  #codeDataType(numCategories: number): string {
    if (numCategories <= 128)  return "int8";
    if (numCategories <= 32768) return "int16";
    return "int32";
  }

  #codeDtype(numCategories: number): string {
    if (numCategories <= 128)  return "|i1";
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

  // ── Zarr v3 metadata helpers ─────────────────────────────────────────────

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
    const dataType = this.#parquetTypeToDataType(colName);
    const numRows = this.#totalRows();
    const maxRowsPerRg = this.#rowsInGroup(0);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: dataType,
      chunk_grid: { name: "regular", configuration: { chunk_shape: [maxRowsPerRg] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: 0,
      codecs: this.#numericCodecs(dataType),
      attributes: { "encoding-type": "array", "encoding-version": "0.2.0" },
    };
  }

  #stringArrayMeta(colName: string, encodingType: string): Record<string, unknown> {
    const numRows = this.#totalRows();
    const maxRowsPerRg = this.#rowsInGroup(0);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: "string",
      chunk_grid: { name: "regular", configuration: { chunk_shape: [maxRowsPerRg] } },
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

  async #codesArrayMeta(colName: string): Promise<Record<string, unknown>> {
    const categories = await this.#getCategories(colName);
    const dataType = this.#codeDataType(categories.length);
    const numRows = this.#totalRows();
    const maxRowsPerRg = this.#rowsInGroup(0);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: dataType,
      chunk_grid: { name: "regular", configuration: { chunk_shape: [maxRowsPerRg] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: 0,
      codecs: this.#numericCodecs(dataType),
      attributes: {},
    };
  }

  async #categoriesArrayMeta(colName: string): Promise<Record<string, unknown>> {
    const categories = await this.#getCategories(colName);
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

  /**
   * Build virtual consolidated metadata matching zarr v3 format.
   * Returns the full root zarr.json with inline consolidated_metadata.
   */
  async getConsolidatedMetadata(): Promise<Record<string, unknown>> {
    await this.#init();

    const catCols = this.#categoricalColumnNames();
    const allCols = this.#columnNames();
    const metadata: Record<string, unknown> = {};

    for (const col of allCols) {
      if (catCols.has(col)) {
        // Categorical column → group with nested consolidated_metadata
        metadata[col] = {
          ...this.#categoricalGroupMeta(),
          consolidated_metadata: { kind: "inline", must_understand: false, metadata: {} },
        };
        // codes and categories arrays
        metadata[`${col}/codes`] = await this.#codesArrayMeta(col);
        metadata[`${col}/categories`] = await this.#categoriesArrayMeta(col);
      } else {
        const isIndex = col === this.#indexColumnName();
        const dtype = this.#parquetTypeToDtype(col);
        if (isIndex || dtype === "|O") {
          metadata[col] = this.#stringArrayMeta(col, "string-array");
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

  async get(key: AbsolutePath, _opts?: unknown): Promise<Uint8Array | undefined> {
    await this.#init();
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

  async #route(key: string): Promise<Uint8Array | undefined> {
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
    const dtype = this.#parquetTypeToDtype(colName);
    const numRgs = this.#numRowGroups();

    // /{col}/zarr.json
    if (parts.length === 2 && parts[1] === "zarr.json") {
      if (isCat) return this.#json(this.#categoricalGroupMeta());
      if (isIndex || dtype === "|O") return this.#json(this.#stringArrayMeta(colName, "string-array"));
      return this.#json(this.#numericArrayMeta(colName));
    }

    // /{col}/c/{rg}
    if (parts.length === 3 && parts[1] === "c" && !isCat) {
      const rgIndex = Number(parts[2]);
      if (Number.isInteger(rgIndex) && rgIndex >= 0 && rgIndex < numRgs) {
        const chunkData = await this.#readColumnChunkForRg(colName, rgIndex);
        if (ArrayBuffer.isView(chunkData)) {
          // Typed array from hyparquet (PLAIN-encoded numerics) — return raw bytes
          const ta = chunkData as ArrayBufferView;
          return new Uint8Array(ta.buffer, ta.byteOffset, ta.byteLength);
        }
        if (dtype === "|O") return this.#encodeVlenUtf8(chunkData as string[]);
        // Plain array of numbers (dictionary-encoded) — encode to typed array
        return this.#encodeTypedArray(chunkData as number[], dtype);
      }
    }

    if (isCat) {
      const subGroup = parts[1];

      if (subGroup === "codes") {
        if (parts.length === 3 && parts[2] === "zarr.json") {
          return this.#json(await this.#codesArrayMeta(colName));
        }
        if (parts.length === 4 && parts[2] === "c") {
          const rgIndex = Number(parts[3]);
          if (Number.isInteger(rgIndex) && rgIndex >= 0 && rgIndex < numRgs) {
            const chunkData = await this.#readColumnChunkForRg(colName, rgIndex);
            const values = Array.isArray(chunkData)
              ? chunkData as string[]
              : Array.from(chunkData as ArrayLike<string>);
            const categories = await this.#getCategories(colName);
            const catMap = new Map(categories.map((c, i) => [c, i]));
            const codes = values.map((v) => catMap.get(v) ?? 0);
            const codeDtype = this.#codeDtype(categories.length);
            return this.#encodeTypedArray(codes, codeDtype);
          }
        }
      }

      if (subGroup === "categories") {
        if (parts.length === 3 && parts[2] === "zarr.json") {
          return this.#json(await this.#categoriesArrayMeta(colName));
        }
        if (parts.length === 4 && parts[2] === "c") {
          const rgIndex = Number(parts[3]);
          if (Number.isInteger(rgIndex) && rgIndex === 0) {
            const categories = await this.#getCategories(colName);
            return this.#encodeVlenUtf8(categories);
          }
        }
      }
    }

    return undefined;
  }
}
