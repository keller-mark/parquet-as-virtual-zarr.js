// @ts-ignore - hyparquet is a JS package; types generated on build
import { parquetMetadata, parquetReadObjects } from "hyparquet";
import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";

/**
 * Virtual Zarr store that exposes a Parquet file as an AnnData-Zarr dataframe.
 *
 * Implements zarrita's AsyncReadable interface. Zarr key paths are mapped to
 * byte ranges in the Parquet file via row-group-level chunking.
 *
 * Key routing:
 *   /.zattrs                          → dataframe group attrs
 *   /.zgroup                          → {zarr_format: 2}
 *   /{col}/.zattrs                    → column encoding attrs
 *   /{col}/.zgroup                    → {zarr_format: 2} (categorical only)
 *   /{col}/.zarray                    → array metadata (non-categorical)
 *   /{col}/{rg}                       → raw chunk bytes (non-categorical)
 *   /{col}/codes/.zattrs              → {}
 *   /{col}/codes/.zarray              → codes array metadata
 *   /{col}/codes/{rg}                 → codes chunk bytes
 *   /{col}/categories/.zattrs         → {}
 *   /{col}/categories/.zarray         → categories array metadata
 *   /{col}/categories/0               → categories chunk bytes (vlen-utf8)
 *
 * The Parquet file is expected to be accessible at key "/" of the inner store.
 * Use `ParquetAsAnnDataFrameZarr.fromStore(store)` to construct an instance.
 */
export class ParquetAsAnnDataFrameZarr implements AsyncReadable {
  readonly #store: AsyncReadable;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #metadata: any = null;
  // hyparquet AsyncBuffer backed by ranged reads through #store
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #asyncBuffer: any = null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #pandasMeta: any = null;
  #categoriesCache = new Map<string, string[]>();

  constructor(store: AsyncReadable) {
    this.#store = store;
  }

  /**
   * Construct a store from any AsyncReadable whose key "/" returns the raw
   * Parquet file bytes (e.g. `new FileSystemStore("/path/to/data.parquet")`).
   */
  static fromStore(store: AsyncReadable): ParquetAsAnnDataFrameZarr {
    return new ParquetAsAnnDataFrameZarr(store);
  }

  /**
   * Lazily initialise: read parquet metadata and build an AsyncBuffer that
   * delegates byte-range reads to the inner store via getRange.
   *
   * A single full read of key "/" is performed once to discover the file's
   * byte length. All subsequent reads (metadata footer, column chunks) use
   * ranged reads, so only the minimal required bytes are fetched.
   */
  async #init(): Promise<void> {
    if (this.#metadata) return;

    if (!this.#store.getRange) {
      throw new Error("ParquetAsAnnDataFrameZarr: inner store must support getRange");
    }

    // Step 1: Read last 8 bytes to discover metadata length and validate magic.
    const tail = await this.#store.getRange("/", { suffixLength: 8 });
    if (!tail) throw new Error("ParquetAsAnnDataFrameZarr: getRange returned nothing for parquet footer tail");
    const tailBuf = tail.buffer.slice(tail.byteOffset, tail.byteOffset + tail.byteLength) as ArrayBuffer;
    const tailView = new DataView(tailBuf);
    // Validate PAR1 magic (last 4 bytes of the file).
    if (tailView.getUint32(4, true) !== 0x31524150) {
      throw new Error("ParquetAsAnnDataFrameZarr: not a valid parquet file (missing PAR1 magic)");
    }
    const metadataLength = tailView.getUint32(0, true);

    // Step 2: Fetch exactly the footer bytes (metadata + 8-byte suffix).
    const footerSize = metadataLength + 8;
    const footerBytes = await this.#store.getRange("/", { suffixLength: footerSize });
    if (!footerBytes) throw new Error("ParquetAsAnnDataFrameZarr: failed to fetch parquet footer");
    const footerBuf = footerBytes.buffer.slice(
      footerBytes.byteOffset,
      footerBytes.byteOffset + footerBytes.byteLength,
    ) as ArrayBuffer;

    this.#metadata = parquetMetadata(footerBuf);

    // AsyncBuffer for hyparquet column reads.
    // byteLength is set to MAX_SAFE_INTEGER because all column-chunk reads
    // supply explicit start/end bounds from parquet metadata — hyparquet never
    // issues an open-ended slice for row-group reads.
    this.#asyncBuffer = {
      byteLength: Number.MAX_SAFE_INTEGER,
      slice: async (start: number, end?: number): Promise<ArrayBuffer> => {
        if (end === undefined) {
          throw new Error("ParquetAsAnnDataFrameZarr: unbounded slice — this is a bug");
        }
        const length = end - start;
        if (length <= 0) return new ArrayBuffer(0);
        const bytes = await this.#store.getRange!("/", { offset: start, length });
        if (!bytes) {
          throw new Error(`ParquetAsAnnDataFrameZarr: getRange returned nothing for offset=${start} length=${length}`);
        }
        return bytes.buffer.slice(
          bytes.byteOffset,
          bytes.byteOffset + bytes.byteLength,
        ) as ArrayBuffer;
      },
    };

    const pandasKv = this.#metadata.key_value_metadata?.find(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (kv: any) => kv.key === "pandas",
    );
    if (pandasKv) {
      this.#pandasMeta = JSON.parse(pandasKv.value);
    }
  }

  /** All column names from parquet schema (leaf elements only). */
  #columnNames(): string[] {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#metadata.schema.filter((s: any) => s.type).map((s: any) => s.name);
  }

  /** Name of the index column (from pandas metadata or first column). */
  #indexColumnName(): string {
    return this.#pandasMeta?.index_columns?.[0] ?? this.#columnNames()[0];
  }

  /** Set of categorical column names (dictionary-encoded strings). */
  #categoricalColumnNames(): Set<string> {
    const cats: string[] =
      this.#pandasMeta?.columns
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        ?.filter((c: any) => c.pandas_type === "categorical")
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .map((c: any) => c.name) ?? [];
    return new Set(cats);
  }

  /** Ordered list of non-index data columns (matches AnnData column-order). */
  #dataColumnOrder(): string[] {
    const indexCol = this.#indexColumnName();
    return this.#columnNames().filter((c) => c !== indexCol);
  }

  /** Map parquet physical type to zarr v2 dtype string. */
  #parquetTypeToDtype(colName: string): string {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const schema = this.#metadata.schema.find((s: any) => s.name === colName);
    if (!schema) return "|O";
    switch (schema.type) {
      case "FLOAT":  return "<f4";
      case "DOUBLE": return "<f8";
      case "INT32":  return "<i4";
      case "INT64":  return "<i8";
      default:       return "|O";
    }
  }

  /** [rowStart, rowEnd) for a given row group index. */
  #rowGroupRange(rgIndex: number): [number, number] {
    let start = 0;
    for (let i = 0; i < rgIndex; i++) {
      start += Number(this.#metadata.row_groups[i].num_rows);
    }
    const end = start + Number(this.#metadata.row_groups[rgIndex].num_rows);
    return [start, end];
  }

  #totalRows(): number {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#metadata.row_groups.reduce((sum: number, rg: any) => sum + Number(rg.num_rows), 0);
  }

  #rowsInGroup(rgIndex: number): number {
    return Number(this.#metadata.row_groups[rgIndex].num_rows);
  }

  /**
   * Read a slice of one column using hyparquet.
   * Only the byte ranges covering [rowStart, rowEnd) are fetched from the store.
   */
  async #readColumnSlice(colName: string, rowStart: number, rowEnd: number): Promise<unknown[]> {
    const rows = (await parquetReadObjects({
      file: this.#asyncBuffer,
      metadata: this.#metadata,
      columns: [colName],
      rowStart,
      rowEnd,
    })) as Record<string, unknown>[];
    return rows.map((r) => r[colName]);
  }

  /**
   * Return sorted unique categories for a categorical column.
   * Reads all row groups once; result is cached for subsequent calls.
   */
  async #getCategories(colName: string): Promise<string[]> {
    if (this.#categoriesCache.has(colName)) {
      return this.#categoriesCache.get(colName)!;
    }
    const allValues = await this.#readColumnSlice(colName, 0, this.#totalRows());
    const categories = [...new Set(allValues as string[])].sort();
    this.#categoriesCache.set(colName, categories);
    return categories;
  }

  /** Encode a numeric array as little-endian typed array bytes. */
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

  /**
   * Encode a string array using zarr v2 vlen-utf8 binary format.
   * Layout: uint32(count) | [uint32(len) | utf8_bytes]...
   */
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

  /** Choose the tightest signed integer dtype for a given category count. */
  #codeDtype(numCategories: number): string {
    if (numCategories <= 128)  return "|i1";
    if (numCategories <= 32768) return "<i2";
    return "<i4";
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
    if (key === "/.zattrs") {
      return this.#json({
        "column-order": this.#dataColumnOrder(),
        _index: this.#indexColumnName(),
        "encoding-type": "dataframe",
        "encoding-version": "0.2.0",
      });
    }

    if (key === "/.zgroup") {
      return this.#json({ zarr_format: 2 });
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
    const numRows = this.#totalRows();
    const numRgs = this.#metadata.row_groups.length;
    // Chunk size equals the size of the first row group (assumed representative).
    const maxRowsPerRg = this.#rowsInGroup(0);

    // /{col}/.zattrs  /{col}/.zgroup  /{col}/.zarray  /{col}/{rg}
    if (parts.length === 2) {
      const subkey = parts[1];

      if (subkey === ".zattrs") {
        if (isCat) {
          return this.#json({ ordered: false, "encoding-type": "categorical", "encoding-version": "0.2.0" });
        }
        if (isIndex) {
          return this.#json({ "encoding-type": "string-array", "encoding-version": "0.2.0" });
        }
        return this.#json({ "encoding-type": "array", "encoding-version": "0.2.0" });
      }

      if (subkey === ".zgroup") {
        return isCat ? this.#json({ zarr_format: 2 }) : undefined;
      }

      if (subkey === ".zarray") {
        if (isCat) return undefined;
        const filters = dtype === "|O" ? [{ id: "vlen-utf8" }] : null;
        return this.#json({
          zarr_format: 2,
          shape: [numRows],
          chunks: [maxRowsPerRg],
          dtype,
          fill_value: dtype === "|O" ? "" : 0,
          order: "C",
          filters,
          compressor: null,
          dimension_separator: ".",
        });
      }

      // Chunk: /{col}/{rgIndex}
      const rgIndex = Number(subkey);
      if (!isCat && Number.isInteger(rgIndex) && rgIndex >= 0 && rgIndex < numRgs) {
        const [rowStart, rowEnd] = this.#rowGroupRange(rgIndex);
        const data = await this.#readColumnSlice(colName, rowStart, rowEnd);
        if (dtype === "|O") return this.#encodeVlenUtf8(data as string[]);
        return this.#encodeTypedArray(data as number[], dtype);
      }
    }

    // /{col}/codes/...  or  /{col}/categories/...
    if (parts.length === 3 && isCat) {
      const subGroup = parts[1];
      const subkey = parts[2];

      if (subGroup === "codes") {
        if (subkey === ".zattrs") return this.#json({});

        if (subkey === ".zarray") {
          const categories = await this.#getCategories(colName);
          const codeDtype = this.#codeDtype(categories.length);
          return this.#json({
            zarr_format: 2,
            shape: [numRows],
            chunks: [maxRowsPerRg],
            dtype: codeDtype,
            fill_value: 0,
            order: "C",
            filters: null,
            compressor: null,
            dimension_separator: ".",
          });
        }

        const rgIndex = Number(subkey);
        if (Number.isInteger(rgIndex) && rgIndex >= 0 && rgIndex < numRgs) {
          const [rowStart, rowEnd] = this.#rowGroupRange(rgIndex);
          const data = await this.#readColumnSlice(colName, rowStart, rowEnd);
          const categories = await this.#getCategories(colName);
          const catMap = new Map(categories.map((c, i) => [c, i]));
          const codes = (data as string[]).map((v) => catMap.get(v) ?? 0);
          const codeDtype = this.#codeDtype(categories.length);
          return this.#encodeTypedArray(codes, codeDtype);
        }
      }

      if (subGroup === "categories") {
        if (subkey === ".zattrs") return this.#json({});

        if (subkey === ".zarray") {
          const categories = await this.#getCategories(colName);
          return this.#json({
            zarr_format: 2,
            shape: [categories.length],
            chunks: [categories.length],
            dtype: "|O",
            fill_value: "",
            order: "C",
            filters: [{ id: "vlen-utf8" }],
            compressor: null,
            dimension_separator: ".",
          });
        }

        const rgIndex = Number(subkey);
        if (Number.isInteger(rgIndex) && rgIndex === 0) {
          const categories = await this.#getCategories(colName);
          return this.#encodeVlenUtf8(categories);
        }
      }
    }

    return undefined;
  }
}
