// @ts-ignore - hyparquet is a JS package; types generated on build
import { parquetMetadata } from "hyparquet";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";
import {
  isZeroCopyEligible,
  extractZeroCopyPageData,
  readColumnChunkData,
  readNullMask,
} from "./vendored/parquet-column.js";
// Side-effect import: registers the snappy codec with zarrita's registry
import "./snappy-codec.js";

interface PartInfo {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  metadata: any;
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
 * How a column is exposed:
 *   categorical → AnnData categorical group (codes + categories)
 *   string      → vlen-utf8 string array (also used for the index column)
 *   numeric     → numeric array (OPTIONAL floats included, with NaN for nulls)
 *   bool        → bool array (REQUIRED BOOLEAN)
 *   nullable    → AnnData nullable group (values + mask)
 */
type ColumnKind = "categorical" | "string" | "numeric" | "bool" | "nullable";

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
 *   /{col}/zarr.json                  → array metadata (string / numeric / bool) or group
 *   /{col}/c/{rg}                     → chunk bytes (string / numeric / bool)
 *   /{col}/zarr.json                  → nullable group (encoding-type: nullable-integer / nullable-boolean / nullable-string-array)
 *   /{col}/values/zarr.json           → values array metadata (nullable columns)
 *   /{col}/values/c/{rg}              → values chunk bytes (nullable columns)
 *   /{col}/mask/zarr.json             → mask array metadata (nullable columns, bool dtype)
 *
 * Nullable encodings follow the AnnData spec (nullable-integer, nullable-boolean,
 * nullable-string-array with na-value "NA"). OPTIONAL FLOAT/DOUBLE columns are plain
 * float arrays with NaN for nulls, since AnnData has no nullable float encoding.
 *   /{col}/mask/c/{rg}               → mask chunk bytes: 1=null, 0=present (nullable columns)
 *   /{col}/codes/zarr.json            → codes array metadata (categorical)
 *   /{col}/codes/c/{rg}              → codes chunk bytes, -1 for missing (categorical)
 *   /{col}/categories/zarr.json       → categories array metadata (categorical)
 *   /{col}/categories/c/0            → categories chunk bytes (vlen-utf8, categorical)
 */
export class ParquetAsAnnDataFrameStore implements AsyncReadable {
  readonly #store: AsyncReadable;
  #initialized = false;
  /** Per-part parquet metadata and store keys. */
  #parts: PartInfo[] = [];
  /** Maps global row group index → part and local row group index. */
  #rgMap: RgMapping[] = [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #pandasMeta: any = null;
  #kinds = new Map<string, ColumnKind>();
  #categoriesCache = new Map<string, string[]>();
  /**
   * Maps column name → zarr codec name for zero-copy compressed pass-through.
   * Only set for columns that are REQUIRED + PLAIN + numeric + compressed with
   * a single data page per column chunk, in every row group of every part.
   * UNCOMPRESSED zero-copy columns are tracked in #zeroCopyRawCols.
   */
  #zeroCopyCompressedCols = new Map<string, string>();
  /** Columns that use zero-copy uncompressed pass-through. */
  #zeroCopyRawCols = new Set<string>();

  constructor(store: AsyncReadable) {
    this.#store = store;
  }

  static fromStore(store: AsyncReadable): ParquetAsAnnDataFrameStore {
    return new ParquetAsAnnDataFrameStore(store);
  }

  /**
   * Read the parquet footer for a given store key and return parsed metadata.
   * Returns null if the key is not a valid parquet file.
   */
  async #readParquetFooter(storeKey: string): Promise<PartInfo | null> {
    if (!this.#store.getRange) {
      throw new Error("ParquetAsAnnDataFrameStore: inner store must support getRange");
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

    return { metadata, storeKey };
  }

  /**
   * Lazily initialise: detect single-file vs multi-part mode, read parquet
   * metadata, and build the global row group mapping.
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
        throw new Error("ParquetAsAnnDataFrameStore: no valid parquet file found (tried '/' and '/part.0.parquet')");
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

    this.#checkRegularChunks();

    // Extract pandas metadata from the first part.
    const firstMeta = this.#parts[0].metadata;
    const pandasKv = firstMeta.key_value_metadata?.find(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (kv: any) => kv.key === "pandas",
    );
    if (pandasKv) {
      this.#pandasMeta = JSON.parse(pandasKv.value);
    }

    this.#classifyColumns();
    this.#detectZeroCopyColumns();

    this.#initialized = true;
  }

  /**
   * The virtual arrays use a regular chunk grid with chunk_shape = rows in
   * row group 0, so every row group except the last must have exactly that
   * many rows (and the last at most that many).
   */
  #checkRegularChunks(): void {
    const numRgs = this.#numRowGroups();
    if (numRgs === 0) return;
    const chunkSize = this.#rowsInGroup(0);
    for (let g = 0; g < numRgs; g++) {
      const rows = this.#rowsInGroup(g);
      const ok = g < numRgs - 1 ? rows === chunkSize : rows > 0 && rows <= chunkSize;
      if (!ok) {
        throw new Error(
          `ParquetAsAnnDataFrameStore: row groups must all have ${chunkSize} rows ` +
            `(except a shorter final one), but row group ${g} has ${rows} rows`,
        );
      }
    }
  }

  /** Map parquet compression codec to zarr codec name for zero-copy. */
  static readonly #PARQUET_TO_ZARR_CODEC: Record<string, string> = {
    SNAPPY: "snappy",
    GZIP: "gzip",
    ZSTD: "zstd",
    LZ4_RAW: "lz4_raw",
    BROTLI: "brotli",
  };

  /**
   * True if the footer's encoding_stats guarantee a single data page and no
   * dictionary page. Without encoding_stats this cannot be known up front.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  static #hasSingleDataPage(meta: any): boolean {
    const stats = meta.encoding_stats;
    if (!stats) return false;
    let dataPages = 0;
    for (const s of stats) {
      if (s.page_type === "DICTIONARY_PAGE") return false;
      if (s.page_type === "DATA_PAGE" || s.page_type === "DATA_PAGE_V2") dataPages += Number(s.count);
    }
    return dataPages === 1;
  }

  /**
   * Check each numeric column against zero-copy criteria (REQUIRED + PLAIN +
   * numeric, same codec everywhere) in every row group of every part, and
   * cache the result along with the compression codec. Compressed columns
   * additionally need a single data page per column chunk, since page payloads
   * cannot be concatenated into one valid compressed stream.
   */
  #detectZeroCopyColumns(): void {
    for (const colName of this.#columnNames()) {
      if (this.#kinds.get(colName) !== "numeric") continue;
      const schemaElement = this.#schemaElement(colName);
      let codec: string | null = null;
      let eligible = true;
      for (let g = 0; g < this.#numRowGroups() && eligible; g++) {
        const { meta } = this.#getColumnMeta(colName, g);
        if (!meta || !isZeroCopyEligible(schemaElement, meta)) eligible = false;
        else if (codec !== null && meta.codec !== codec) eligible = false;
        else if (meta.codec !== "UNCOMPRESSED" && !ParquetAsAnnDataFrameStore.#hasSingleDataPage(meta)) eligible = false;
        else codec = meta.codec;
      }
      if (!eligible || codec === null) continue;
      if (codec === "UNCOMPRESSED") {
        this.#zeroCopyRawCols.add(colName);
      } else {
        const zarrCodec = ParquetAsAnnDataFrameStore.#PARQUET_TO_ZARR_CODEC[codec];
        if (zarrCodec) this.#zeroCopyCompressedCols.set(colName, zarrCodec);
      }
    }
  }

  /** Assign each column the kind it is exposed as (see ColumnKind). */
  #classifyColumns(): void {
    const catCols = this.#categoricalColumnNames();
    const indexCol = this.#indexColumnName();
    for (const col of this.#columnNames()) {
      const schema = this.#schemaElement(col);
      let kind: ColumnKind;
      if (catCols.has(col)) kind = "categorical";
      else if (col === indexCol) kind = "string";
      else if (schema.repetition_type === "OPTIONAL" && !this.#isFloat(col)) kind = "nullable";
      else if (this.#parquetTypeToDtype(col) !== "|O") kind = "numeric";
      else if (schema.type === "BOOLEAN") kind = "bool";
      else kind = "string";
      this.#kinds.set(col, kind);
    }
  }

  /** Schema from first part (all parts share the same schema). */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  get #schema(): any[] {
    return this.#parts[0].metadata.schema;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #schemaElement(colName: string): any {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#schema.find((s: any) => s.type && s.name === colName);
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
    switch (this.#schemaElement(colName)?.type) {
      case "FLOAT":  return "float32";
      case "DOUBLE": return "float64";
      case "INT32":  return "int32";
      case "INT64":  return "int64";
      default:       return "string";
    }
  }

  #parquetTypeToDtype(colName: string): string {
    switch (this.#schemaElement(colName)?.type) {
      case "FLOAT":  return "<f4";
      case "DOUBLE": return "<f8";
      case "INT32":  return "<i4";
      case "INT64":  return "<i8";
      default:       return "|O";
    }
  }

  #isFloat(colName: string): boolean {
    const type = this.#schemaElement(colName)?.type;
    return type === "FLOAT" || type === "DOUBLE";
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

  // ── Column chunk byte reading ─────────────────────────────────────────

  /**
   * Get the column metadata for a specific column in a specific row group.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #getColumnMeta(colName: string, rgIndex: number): { part: PartInfo; meta: any } {
    const mapping = this.#rgMap[rgIndex];
    const part = this.#parts[mapping.partIndex];
    const rg = part.metadata.row_groups[mapping.localRgIndex];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const colChunk = rg.columns.find((c: any) => c.meta_data.path_in_schema[0] === colName);
    return { part, meta: colChunk?.meta_data };
  }

  /**
   * Fetch raw column chunk bytes from the underlying store via getRange.
   * The returned bytes include all page headers and page data for the column chunk.
   */
  async #fetchColumnChunkBytes(colName: string, rgIndex: number): Promise<Uint8Array> {
    const { part, meta } = this.#getColumnMeta(colName, rgIndex);
    const columnOffset = Number(meta.dictionary_page_offset || meta.data_page_offset);
    const chunkSize = Number(meta.total_compressed_size);
    const bytes = await this.#store.getRange!(part.storeKey as AbsolutePath, {
      offset: columnOffset,
      length: chunkSize,
    });
    if (!bytes) {
      throw new Error(
        `ParquetAsAnnDataFrameStore: getRange returned nothing for column=${colName} rg=${rgIndex}`,
      );
    }
    return bytes;
  }

  /** Decode one column chunk to one value per row (nulls → placeholders). */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async #decodeColumnChunk(colName: string, rawBytes: Uint8Array, rgIndex: number): Promise<any> {
    const { meta } = this.#getColumnMeta(colName, rgIndex);
    const isOptional = this.#schemaElement(colName)?.repetition_type !== "REQUIRED";
    return await readColumnChunkData(rawBytes, meta.codec, meta.type, isOptional);
  }

  /**
   * Read one column from a specific row group as the bytes of a Zarr chunk
   * (for numeric, bool, and nullable-values arrays).
   *
   * For zero-copy eligible columns (REQUIRED + PLAIN + numeric):
   *   Returns raw page bytes (compressed or uncompressed) with page headers
   *   stripped. The zarr codec pipeline handles decompression.
   *
   * For other columns:
   *   Decodes the column data using vendored parquet decoding logic and
   *   returns little-endian bytes, or vlen-utf8 bytes for strings.
   */
  async #readValuesChunk(colName: string, rgIndex: number): Promise<Uint8Array> {
    const rawBytes = await this.#fetchColumnChunkBytes(colName, rgIndex);

    // Zero-copy path: return raw page data bytes directly
    const zarrCodec = this.#zeroCopyCompressedCols.get(colName);
    if (zarrCodec || this.#zeroCopyRawCols.has(colName)) {
      const page = extractZeroCopyPageData(rawBytes);
      const v2 = page?.header.data_page_header_v2;
      const uncompressedV2 = page?.header.type === "DATA_PAGE_V2" && v2?.is_compressed === false;
      if (page && !(zarrCodec && uncompressedV2)) {
        if (zarrCodec === "lz4_raw") {
          // LZ4 raw blocks need a size prefix so the zarr codec knows the output size.
          // Prepend 4-byte LE uncompressed size (from page header).
          const prefixed = new Uint8Array(4 + page.data.byteLength);
          new DataView(prefixed.buffer).setUint32(0, page.header.uncompressed_page_size, true);
          prefixed.set(page.data, 4);
          return prefixed;
        }
        return page.data;
      }
      if (zarrCodec) {
        // The metadata declares a compression codec, so decoded bytes would be
        // misread. encoding_stats promised a single compressed data page.
        throw new Error(
          `ParquetAsAnnDataFrameStore: column=${colName} rg=${rgIndex} does not contain ` +
            `a single compressed data page, contradicting its encoding_stats`,
        );
      }
      // Uncompressed multi-page fallback: decoded bytes match the [bytes] pipeline
    }

    const data = await this.#decodeColumnChunk(colName, rawBytes, rgIndex);
    if (ArrayBuffer.isView(data)) {
      return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    return this.#encodeVlenUtf8(data as (string | null)[]);
  }

  /** Read a string-kind chunk, stringifying non-string values (numeric index). */
  async #readStringChunk(colName: string, rgIndex: number): Promise<Uint8Array> {
    const rawBytes = await this.#fetchColumnChunkBytes(colName, rgIndex);
    const data = await this.#decodeColumnChunk(colName, rawBytes, rgIndex);
    const strings = Array.from(data as ArrayLike<unknown>, (v) =>
      v === null || v === undefined ? "" : typeof v === "string" ? v : String(v),
    );
    return this.#encodeVlenUtf8(strings);
  }

  /**
   * Decode one row group of a categorical column, with null (missing) at
   * positions whose definition level is 0.
   */
  async #readCategoricalValues(colName: string, rgIndex: number): Promise<unknown[]> {
    const rawBytes = await this.#fetchColumnChunkBytes(colName, rgIndex);
    const data = await this.#decodeColumnChunk(colName, rawBytes, rgIndex);
    const values: unknown[] = Array.from(data as ArrayLike<unknown>);
    const schema = this.#schemaElement(colName);
    // Strings already decode to null at missing positions; numeric
    // placeholders (NaN / 0) need the definition levels to tell them apart.
    if (schema?.repetition_type === "OPTIONAL" && schema.type !== "BYTE_ARRAY") {
      const { meta } = this.#getColumnMeta(colName, rgIndex);
      const mask = await readNullMask(rawBytes, meta.codec, this.#rowsInGroup(rgIndex));
      for (let i = 0; i < values.length; i++) if (mask[i]) values[i] = null;
    }
    return values;
  }

  async #getCategories(colName: string): Promise<string[]> {
    if (this.#categoriesCache.has(colName)) {
      return this.#categoriesCache.get(colName)!;
    }
    const distinct = new Set<unknown>();
    for (let rg = 0; rg < this.#numRowGroups(); rg++) {
      for (const v of await this.#readCategoricalValues(colName, rg)) {
        if (v !== null) distinct.add(v);
      }
    }
    const categories = [...distinct].sort() as string[];
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

  #numericCodecs(colName: string, dataType: string): unknown[] {
    const bytesCodec = dataType === "int8"
      ? { name: "bytes" }
      : { name: "bytes", configuration: { endian: "little" } };
    const zarrCodec = this.#zeroCopyCompressedCols.get(colName);
    if (zarrCodec) {
      return [bytesCodec, { name: zarrCodec }];
    }
    return [bytesCodec];
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
      codecs: this.#numericCodecs(colName, dataType),
      attributes: { "encoding-type": "array", "encoding-version": "0.2.0" },
    };
  }

  #stringArrayMeta(encodingType: string): Record<string, unknown> {
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

  /**
   * Bool array, used for REQUIRED BOOLEAN columns and for the values and mask
   * members of nullable groups. AnnData writes all of these as encoding-type "array".
   */
  #boolArrayMeta(): Record<string, unknown> {
    const numRows = this.#totalRows();
    const maxRowsPerRg = this.#rowsInGroup(0);
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [numRows],
      data_type: "bool",
      chunk_grid: { name: "regular", configuration: { chunk_shape: [maxRowsPerRg] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: false,
      codecs: [{ name: "bytes", configuration: { endian: "little" } }],
      attributes: { "encoding-type": "array", "encoding-version": "0.2.0" },
    };
  }

  /**
   * AnnData nullable encoding-type string for a column. OPTIONAL FLOAT/DOUBLE
   * columns never reach here: AnnData has no nullable float encoding, so they
   * are plain float arrays with NaN for nulls.
   */
  #nullableEncodingType(colName: string): string {
    if (this.#schemaElement(colName)?.type === "BOOLEAN") return "nullable-boolean";
    if (this.#parquetTypeToDtype(colName) === "|O") return "nullable-string-array";
    return "nullable-integer";
  }

  #nullableGroupMeta(encodingType: string): Record<string, unknown> {
    return {
      zarr_format: 3,
      node_type: "group",
      attributes: {
        // Parquet nulls compare as missing (NA), not as a never-equal value (NaN)
        ...(encodingType === "nullable-string-array" ? { "na-value": "NA" } : {}),
        "encoding-type": encodingType,
        "encoding-version": "0.1.0",
      },
    };
  }

  #nullableValuesMeta(colName: string): Record<string, unknown> {
    const encodingType = this.#nullableEncodingType(colName);
    if (encodingType === "nullable-boolean") return this.#boolArrayMeta();
    if (encodingType === "nullable-string-array") return this.#stringArrayMeta("string-array");
    return this.#numericArrayMeta(colName);
  }

  async #readMaskForRg(colName: string, rgIndex: number): Promise<Uint8Array> {
    const rawBytes = await this.#fetchColumnChunkBytes(colName, rgIndex);
    const { meta } = this.#getColumnMeta(colName, rgIndex);
    const numRows = this.#rowsInGroup(rgIndex);
    return readNullMask(rawBytes, meta.codec, numRows);
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
      codecs: this.#numericCodecs(colName, dataType),
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

  /** Metadata document at /{col}/zarr.json. */
  #columnMeta(colName: string): Record<string, unknown> {
    switch (this.#kinds.get(colName)) {
      case "categorical": return this.#categoricalGroupMeta();
      case "nullable":    return this.#nullableGroupMeta(this.#nullableEncodingType(colName));
      case "numeric":     return this.#numericArrayMeta(colName);
      case "bool":        return this.#boolArrayMeta();
      default:            return this.#stringArrayMeta("string-array");
    }
  }

  /**
   * Build virtual consolidated metadata matching zarr v3 format.
   * Returns the full root zarr.json with inline consolidated_metadata.
   */
  async getConsolidatedMetadata(): Promise<Record<string, unknown>> {
    await this.#init();

    const metadata: Record<string, unknown> = {};

    for (const col of this.#columnNames()) {
      const kind = this.#kinds.get(col);
      if (kind === "categorical" || kind === "nullable") {
        metadata[col] = {
          ...this.#columnMeta(col),
          consolidated_metadata: { kind: "inline", must_understand: false, metadata: {} },
        };
      } else {
        metadata[col] = this.#columnMeta(col);
      }
      if (kind === "categorical") {
        metadata[`${col}/codes`] = await this.#codesArrayMeta(col);
        metadata[`${col}/categories`] = await this.#categoriesArrayMeta(col);
      } else if (kind === "nullable") {
        metadata[`${col}/values`] = this.#nullableValuesMeta(col);
        metadata[`${col}/mask`] = this.#boolArrayMeta();
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

  /** Parse a chunk index, returning undefined unless it is a valid row group index. */
  #rgIndex(part: string): number | undefined {
    const rgIndex = Number(part);
    return Number.isInteger(rgIndex) && rgIndex >= 0 && rgIndex < this.#numRowGroups()
      ? rgIndex
      : undefined;
  }

  async #route(key: string): Promise<Uint8Array | undefined> {
    if (key === "/zarr.json") {
      return this.#json(this.#rootGroupMeta());
    }

    const parts = key.split("/").filter(Boolean);
    if (parts.length < 2) return undefined;

    const colName = parts[0];
    const kind = this.#kinds.get(colName);
    if (!kind) return undefined;

    // /{col}/zarr.json
    if (parts.length === 2 && parts[1] === "zarr.json") {
      return this.#json(this.#columnMeta(colName));
    }

    // /{col}/c/{rg} — array-kind columns only
    if (parts.length === 3 && parts[1] === "c") {
      const rgIndex = this.#rgIndex(parts[2]);
      if (rgIndex === undefined) return undefined;
      if (kind === "string") return this.#readStringChunk(colName, rgIndex);
      if (kind === "numeric" || kind === "bool") return this.#readValuesChunk(colName, rgIndex);
      return undefined;
    }

    if (parts.length !== 3 && parts.length !== 4) return undefined;
    const subGroup = parts[1];
    const isMeta = parts.length === 3 && parts[2] === "zarr.json";
    const rgIndex = parts.length === 4 && parts[2] === "c" ? this.#rgIndex(parts[3]) : undefined;

    // /{col}/values/… and /{col}/mask/… — nullable columns
    if (kind === "nullable") {
      if (subGroup === "values") {
        if (isMeta) return this.#json(this.#nullableValuesMeta(colName));
        if (rgIndex !== undefined) return this.#readValuesChunk(colName, rgIndex);
      }
      if (subGroup === "mask") {
        if (isMeta) return this.#json(this.#boolArrayMeta());
        if (rgIndex !== undefined) return this.#readMaskForRg(colName, rgIndex);
      }
    }

    if (kind === "categorical") {
      if (subGroup === "codes") {
        if (isMeta) return this.#json(await this.#codesArrayMeta(colName));
        if (rgIndex !== undefined) {
          const values = await this.#readCategoricalValues(colName, rgIndex);
          const categories = await this.#getCategories(colName);
          const catMap = new Map<unknown, number>(categories.map((c, i) => [c, i]));
          // AnnData uses -1 for missing categorical values
          const codes = values.map((v) => (v === null ? -1 : catMap.get(v) ?? -1));
          return this.#encodeTypedArray(codes, this.#codeDtype(categories.length));
        }
      }

      if (subGroup === "categories") {
        if (isMeta) return this.#json(await this.#categoriesArrayMeta(colName));
        if (parts.length === 4 && parts[2] === "c" && parts[3] === "0") {
          return this.#encodeVlenUtf8(await this.#getCategories(colName));
        }
      }
    }

    return undefined;
  }
}
