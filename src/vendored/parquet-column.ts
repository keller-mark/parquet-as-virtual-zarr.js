/**
 * Minimal parquet page parsing and column data reading.
 * Vendored from hyparquet (MIT license).
 *
 * Supports:
 * - Page header parsing (Thrift compact protocol)
 * - PLAIN encoding for numeric types and BYTE_ARRAY
 * - RLE/bit-packed hybrid encoding (for dictionary indices and definition levels)
 * - Snappy and UNCOMPRESSED codecs
 * - Dictionary-encoded columns (RLE_DICTIONARY / PLAIN_DICTIONARY)
 */

import {
  type DataReader,
  createReader,
  deserializeTCompactProtocol,
  readVarInt,
} from "./thrift.js";
import { snappyUncompress } from "./snappy.js";
import { lz4RawDecompress } from "./lz4raw.js";

// ── Constants ──────────────────────────────────────────────────────────

const PageTypes = [
  "DATA_PAGE",
  "INDEX_PAGE",
  "DICTIONARY_PAGE",
  "DATA_PAGE_V2",
] as const;
const Encodings = [
  "PLAIN",
  "GROUP_VAR_INT",
  "PLAIN_DICTIONARY",
  "RLE",
  "BIT_PACKED",
  "DELTA_BINARY_PACKED",
  "DELTA_LENGTH_BYTE_ARRAY",
  "DELTA_BYTE_ARRAY",
  "RLE_DICTIONARY",
  "BYTE_STREAM_SPLIT",
] as const;

// ── Page header ────────────────────────────────────────────────────────

export interface PageHeader {
  type: string;
  uncompressed_page_size: number;
  compressed_page_size: number;
  data_page_header?: {
    num_values: number;
    encoding: string;
  };
  dictionary_page_header?: {
    num_values: number;
    encoding: string;
  };
  data_page_header_v2?: {
    num_values: number;
    num_nulls: number;
    num_rows: number;
    encoding: string;
    definition_levels_byte_length: number;
    repetition_levels_byte_length: number;
    is_compressed: boolean;
  };
}

export function parsePageHeader(reader: DataReader): PageHeader {
  const header = deserializeTCompactProtocol(reader);
  return {
    type: PageTypes[header.field_1],
    uncompressed_page_size: header.field_2,
    compressed_page_size: header.field_3,
    data_page_header: header.field_5 && {
      num_values: header.field_5.field_1,
      encoding: Encodings[header.field_5.field_2],
    },
    dictionary_page_header: header.field_7 && {
      num_values: header.field_7.field_1,
      encoding: Encodings[header.field_7.field_2],
    },
    data_page_header_v2: header.field_8 && {
      num_values: header.field_8.field_1,
      num_nulls: header.field_8.field_2,
      num_rows: header.field_8.field_3,
      encoding: Encodings[header.field_8.field_4],
      definition_levels_byte_length: header.field_8.field_5,
      repetition_levels_byte_length: header.field_8.field_6,
      is_compressed:
        header.field_8.field_7 === undefined ? true : header.field_8.field_7,
    },
  };
}

// ── Decompression ──────────────────────────────────────────────────────

async function decompressPage(
  compressedBytes: Uint8Array,
  uncompressedPageSize: number,
  codec: string,
): Promise<Uint8Array> {
  if (codec === "UNCOMPRESSED") {
    return compressedBytes;
  }
  if (codec === "SNAPPY") {
    const output = new Uint8Array(uncompressedPageSize);
    snappyUncompress(compressedBytes, output);
    return output;
  }
  if (codec === "GZIP") {
    return decompressGzip(compressedBytes);
  }
  if (codec === "ZSTD") {
    return decompressZstd(compressedBytes, uncompressedPageSize);
  }
  if (codec === "LZ4_RAW") {
    const output = new Uint8Array(uncompressedPageSize);
    lz4RawDecompress(compressedBytes, output);
    return output;
  }
  if (codec === "BROTLI") {
    return decompressBrotli(compressedBytes);
  }
  throw new Error(`parquet unsupported compression codec: ${codec}`);
}

async function decompressGzip(bytes: Uint8Array): Promise<Uint8Array> {
  // Use the web-standard DecompressionStream (Node 18+ and browsers)
  const ds = new DecompressionStream("gzip");
  const writer = ds.writable.getWriter();
  const reader = ds.readable.getReader();
  writer.write(bytes.buffer instanceof SharedArrayBuffer ? new Uint8Array(bytes) : bytes as unknown as BufferSource);
  writer.close();
  const chunks: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  if (chunks.length === 1) return chunks[0];
  const totalLen = chunks.reduce((s, c) => s + c.byteLength, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const c of chunks) {
    result.set(c, offset);
    offset += c.byteLength;
  }
  return result;
}

async function decompressZstd(
  bytes: Uint8Array,
  _uncompressedSize: number,
): Promise<Uint8Array> {
  // Use numcodecs/zstd (WASM) — available as a transitive dependency via zarrita
  const { default: ZstdCodec } = await import("numcodecs/zstd");
  const codec = ZstdCodec.fromConfig({});
  return codec.decode(bytes);
}

async function decompressBrotli(bytes: Uint8Array): Promise<Uint8Array> {
  // Use Node.js zlib (brotli not available in DecompressionStream)
  try {
    const zlib = await import("node:zlib");
    const result = zlib.brotliDecompressSync(bytes);
    return new Uint8Array(result.buffer, result.byteOffset, result.byteLength);
  } catch {
    throw new Error(
      "parquet BROTLI decompression requires Node.js zlib module. " +
        "Brotli is not supported in browser environments without a polyfill.",
    );
  }
}

// ── RLE / Bit-packed hybrid ────────────────────────────────────────────

function bitWidth(value: number): number {
  return 32 - Math.clz32(value);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function readRleBitPackedHybrid(
  reader: DataReader,
  width: number,
  output: number[],
  length?: number,
): void {
  if (length === undefined) {
    length = reader.view.getUint32(reader.offset, true);
    reader.offset += 4;
  }
  const startOffset = reader.offset;
  let seen = 0;

  while (seen < output.length) {
    const header = readVarInt(reader);
    if (header & 1) {
      seen = readBitPacked(reader, header, width, output, seen);
    } else {
      const count = header >>> 1;
      readRle(reader, count, width, output, seen);
      seen += count;
    }
  }
  reader.offset = startOffset + length;
}

function readRle(
  reader: DataReader,
  count: number,
  bw: number,
  output: number[],
  seen: number,
): void {
  const width = (bw + 7) >> 3;
  let value = 0;
  for (let i = 0; i < width; i++) {
    value |= reader.view.getUint8(reader.offset++) << (i << 3);
  }
  for (let i = 0; i < count; i++) {
    output[seen + i] = value;
  }
}

function readBitPacked(
  reader: DataReader,
  header: number,
  bw: number,
  output: number[],
  seen: number,
): number {
  let count = (header >> 1) << 3;
  const mask = (1 << bw) - 1;

  let data = 0;
  if (reader.offset < reader.view.byteLength) {
    data = reader.view.getUint8(reader.offset++);
  } else if (mask) {
    throw new Error(`parquet bitpack offset ${reader.offset} out of range`);
  }
  let left = 8;
  let right = 0;

  while (count) {
    if (right > 8) {
      right -= 8;
      left -= 8;
      data >>>= 8;
    } else if (left - right < bw) {
      data |= reader.view.getUint8(reader.offset) << left;
      reader.offset++;
      left += 8;
    } else {
      if (seen < output.length) {
        output[seen++] = (data >> right) & mask;
      }
      count--;
      right += bw;
    }
  }

  return seen;
}

// ── PLAIN readers ──────────────────────────────────────────────────────

function align(buffer: ArrayBufferLike, offset: number, size: number): ArrayBuffer {
  const aligned = new ArrayBuffer(size);
  new Uint8Array(aligned).set(new Uint8Array(buffer, offset, size));
  return aligned;
}

function readPlainInt32(reader: DataReader, count: number): Int32Array {
  const byteOffset = reader.view.byteOffset + reader.offset;
  const values =
    byteOffset % 4
      ? new Int32Array(align(reader.view.buffer, byteOffset, count * 4))
      : new Int32Array(reader.view.buffer, byteOffset, count);
  reader.offset += count * 4;
  return values;
}

function readPlainInt64(reader: DataReader, count: number): BigInt64Array {
  const byteOffset = reader.view.byteOffset + reader.offset;
  const values =
    byteOffset % 8
      ? new BigInt64Array(align(reader.view.buffer, byteOffset, count * 8))
      : new BigInt64Array(reader.view.buffer, byteOffset, count);
  reader.offset += count * 8;
  return values;
}

function readPlainFloat(reader: DataReader, count: number): Float32Array {
  const byteOffset = reader.view.byteOffset + reader.offset;
  const values =
    byteOffset % 4
      ? new Float32Array(align(reader.view.buffer, byteOffset, count * 4))
      : new Float32Array(reader.view.buffer, byteOffset, count);
  reader.offset += count * 4;
  return values;
}

function readPlainDouble(reader: DataReader, count: number): Float64Array {
  const byteOffset = reader.view.byteOffset + reader.offset;
  const values =
    byteOffset % 8
      ? new Float64Array(align(reader.view.buffer, byteOffset, count * 8))
      : new Float64Array(reader.view.buffer, byteOffset, count);
  reader.offset += count * 8;
  return values;
}

function readPlainByteArray(reader: DataReader, count: number): Uint8Array[] {
  const values = new Array<Uint8Array>(count);
  for (let i = 0; i < count; i++) {
    const length = reader.view.getUint32(reader.offset, true);
    reader.offset += 4;
    values[i] = new Uint8Array(
      reader.view.buffer,
      reader.view.byteOffset + reader.offset,
      length,
    );
    reader.offset += length;
  }
  return values;
}

type DecodedArray = Int32Array | BigInt64Array | Float32Array | Float64Array | Uint8Array | Uint8Array[] | (string | null)[];

function readPlainBoolean(reader: DataReader, count: number): Uint8Array {
  const byteCount = Math.ceil(count / 8);
  const result = new Uint8Array(count);
  for (let i = 0; i < count; i++) {
    const byte = reader.view.getUint8(reader.offset + Math.floor(i / 8));
    result[i] = (byte >> (i % 8)) & 1;
  }
  reader.offset += byteCount;
  return result;
}

function readPlainValues(
  reader: DataReader,
  parquetType: string,
  count: number,
): DecodedArray {
  switch (parquetType) {
    case "INT32":
      return readPlainInt32(reader, count);
    case "INT64":
      return readPlainInt64(reader, count);
    case "FLOAT":
      return readPlainFloat(reader, count);
    case "DOUBLE":
      return readPlainDouble(reader, count);
    case "BYTE_ARRAY":
      return readPlainByteArray(reader, count);
    case "BOOLEAN":
      return readPlainBoolean(reader, count);
    default:
      throw new Error(`parquet unsupported PLAIN type: ${parquetType}`);
  }
}

// ── Column chunk reader ────────────────────────────────────────────────

const decoder = new TextDecoder();

/**
 * Determine if a column can use zero-copy pass-through for its data.
 * Zero-copy is possible when:
 * - Column is REQUIRED (no definition levels to strip)
 * - Encoding is PLAIN (not dictionary)
 * - Type is numeric (INT32, INT64, FLOAT, DOUBLE)
 */
export function isZeroCopyEligible(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  schemaElement: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  columnMeta: any,
): boolean {
  // Must be numeric
  const numericTypes = ["FLOAT", "DOUBLE", "INT32", "INT64"];
  if (!numericTypes.includes(schemaElement.type)) return false;

  // Must be REQUIRED (no definition levels in page data)
  if (schemaElement.repetition_type !== "REQUIRED") return false;

  // Must not use dictionary encoding
  const encodings: string[] = columnMeta.encodings || [];
  if (
    encodings.includes("RLE_DICTIONARY") ||
    encodings.includes("PLAIN_DICTIONARY")
  ) {
    return false;
  }

  return true;
}

/**
 * Extract raw page data from a column chunk for zero-copy pass-through.
 * Returns the page data bytes with page headers stripped, along with the
 * page header (needed for the uncompressed size and V2 compression flag).
 *
 * For compressed columns: returns the compressed page data (a valid stream
 *   for the corresponding zarr codec).
 * For UNCOMPRESSED columns: returns the raw bytes (the typed array values).
 *
 * Only works for single-data-page column chunks (no dictionary pages).
 * Returns null if multiple data pages are found (caller should fall back to
 * readColumnChunkData).
 */
export function extractZeroCopyPageData(
  rawBytes: Uint8Array,
): { data: Uint8Array; header: PageHeader } | null {
  const reader = createReader(rawBytes);
  let result: { data: Uint8Array; header: PageHeader } | null = null;

  while (reader.offset < rawBytes.byteLength) {
    const header = parsePageHeader(reader);
    const pageBytes = new Uint8Array(
      rawBytes.buffer,
      rawBytes.byteOffset + reader.offset,
      header.compressed_page_size,
    );
    reader.offset += header.compressed_page_size;

    if (header.type === "DATA_PAGE" || header.type === "DATA_PAGE_V2") {
      if (result !== null) {
        // Multiple data pages — cannot do zero-copy
        return null;
      }
      result = { data: pageBytes, header };
    }
  }

  return result;
}

/**
 * Expand a compact (non-null-only) array back to full length using definition levels.
 * Null positions receive a type-appropriate placeholder: NaN for floats, 0 for
 * ints and booleans, null for strings.
 */
function expandNullable(
  compact: DecodedArray,
  defLevels: number[],
  parquetType: string,
): DecodedArray {
  const total = defLevels.length;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let result: any;
  if (parquetType === "FLOAT") {
    result = new Float32Array(total).fill(NaN);
  } else if (parquetType === "DOUBLE") {
    result = new Float64Array(total).fill(NaN);
  } else if (parquetType === "INT32") {
    result = new Int32Array(total);
  } else if (parquetType === "INT64") {
    result = new BigInt64Array(total);
  } else if (parquetType === "BOOLEAN") {
    result = new Uint8Array(total);
  } else {
    result = new Array<string | null>(total).fill(null);
  }
  let j = 0;
  for (let i = 0; i < total; i++) {
    if (defLevels[i] !== 0) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      result[i] = (compact as any)[j++];
    }
  }
  return result as DecodedArray;
}

/**
 * Decode the values section of one data page (after any definition levels)
 * and expand it to one value per row when definition levels are given.
 */
function decodePageValues(
  reader: DataReader,
  pageEnd: number,
  encoding: string,
  parquetType: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dictionary: any,
  numValues: number,
  defLevels: number[] | undefined,
): DecodedArray {
  const nonNullCount = defLevels
    ? defLevels.reduce((acc, l) => acc + (l !== 0 ? 1 : 0), 0)
    : numValues;

  let compact: DecodedArray;
  if (encoding === "PLAIN") {
    compact = readPlainValues(reader, parquetType, nonNullCount);
    if (parquetType === "BYTE_ARRAY") {
      compact = (compact as Uint8Array[]).map((b) => decoder.decode(b));
    }
  } else if (encoding === "RLE_DICTIONARY" || encoding === "PLAIN_DICTIONARY") {
    if (!dictionary) throw new Error("parquet dictionary-encoded page without a dictionary page");
    const bw = reader.view.getUint8(reader.offset);
    reader.offset += 1;
    // bitWidth 0 = every value is the first dictionary entry
    const indices = new Array<number>(nonNullCount).fill(0);
    if (bw) readRleBitPackedHybrid(reader, bw, indices, pageEnd - reader.offset);
    if (ArrayBuffer.isView(dictionary)) {
      // Numeric dictionary → typed array result
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const Ctor = (dictionary as any).constructor as {
        new (len: number): Int32Array | Float32Array | Float64Array | BigInt64Array | Uint8Array;
      };
      const result = new Ctor(nonNullCount);
      for (let i = 0; i < nonNullCount; i++) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (result as any)[i] = (dictionary as any)[indices[i]];
      }
      compact = result;
    } else {
      compact = indices.map((idx) => dictionary[idx]);
    }
  } else {
    throw new Error(`parquet unsupported encoding: ${encoding}`);
  }

  return defLevels ? expandNullable(compact, defLevels, parquetType) : compact;
}

/**
 * Read and decode all values from a column chunk's raw bytes.
 * Handles dictionary-encoded, PLAIN-encoded, and optional (nullable) columns.
 * Returns typed array for numerics/booleans, string array for strings
 * (with null at missing positions for optional columns).
 */
export async function readColumnChunkData(
  rawBytes: Uint8Array,
  codec: string,
  parquetType: string,
  isOptional: boolean,
): Promise<DecodedArray> {
  const reader = createReader(rawBytes);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let dictionary: any;
  const allValues: DecodedArray[] = [];

  while (reader.offset < rawBytes.byteLength) {
    const header = parsePageHeader(reader);
    const pageBytes = new Uint8Array(
      rawBytes.buffer,
      rawBytes.byteOffset + reader.offset,
      header.compressed_page_size,
    );
    reader.offset += header.compressed_page_size;

    if (header.type === "DICTIONARY_PAGE") {
      const diph = header.dictionary_page_header!;
      const page = await decompressPage(
        pageBytes,
        header.uncompressed_page_size,
        codec,
      );
      const pageReader = createReader(page);
      const raw = readPlainValues(pageReader, parquetType, diph.num_values);
      // Convert BYTE_ARRAY to strings for dictionary
      if (parquetType === "BYTE_ARRAY") {
        dictionary = (raw as Uint8Array[]).map((b) => decoder.decode(b));
      } else {
        dictionary = raw;
      }
    } else if (header.type === "DATA_PAGE") {
      const daph = header.data_page_header!;
      const page = await decompressPage(
        pageBytes,
        header.uncompressed_page_size,
        codec,
      );
      const pageReader = createReader(page);

      // Decode definition levels for OPTIONAL columns to determine which rows are non-null.
      // Definition levels are RLE/bit-packed with bit-width 1 (0 = null, 1 = defined).
      let defLevels: number[] | undefined;
      if (isOptional) {
        const defLength = pageReader.view.getUint32(pageReader.offset, true);
        pageReader.offset += 4;
        defLevels = new Array<number>(daph.num_values).fill(1);
        if (defLength > 0) {
          readRleBitPackedHybrid(pageReader, 1, defLevels, defLength);
        }
      }
      allValues.push(decodePageValues(
        pageReader, page.byteLength, daph.encoding, parquetType, dictionary, daph.num_values, defLevels,
      ));
    } else if (header.type === "DATA_PAGE_V2") {
      const daph2 = header.data_page_header_v2!;
      const repLen = daph2.repetition_levels_byte_length;
      const defLen = daph2.definition_levels_byte_length;

      // For V2, repetition and definition levels are stored uncompressed
      // before the (possibly compressed) values
      let defLevels: number[] | undefined;
      if (isOptional) {
        defLevels = new Array<number>(daph2.num_values).fill(1);
        if (defLen > 0) {
          const defReader = createReader(pageBytes.subarray(repLen, repLen + defLen));
          readRleBitPackedHybrid(defReader, 1, defLevels, defLen);
        }
      }

      const levelsLength = repLen + defLen;
      const valuesCompressed = pageBytes.subarray(levelsLength);
      const uncompressedPageSize = header.uncompressed_page_size - levelsLength;
      const page = daph2.is_compressed !== false
        ? await decompressPage(valuesCompressed, uncompressedPageSize, codec)
        : valuesCompressed;
      const pageReader = createReader(page);
      allValues.push(decodePageValues(
        pageReader, page.byteLength, daph2.encoding, parquetType, dictionary,
        daph2.num_values - daph2.num_nulls, defLevels,
      ));
    }
  }

  // Concatenate results
  if (allValues.length === 0) return [];
  if (allValues.length === 1) return allValues[0];

  if (ArrayBuffer.isView(allValues[0])) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const Ctor = (allValues[0] as any).constructor as {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      new (len: number): any;
    };
    const totalLen = allValues.reduce(
      (s: number, a: { length: number }) => s + a.length,
      0,
    );
    const result = new Ctor(totalLen);
    let offset = 0;
    for (const a of allValues) {
      result.set(a, offset);
      offset += a.length;
    }
    return result;
  }

  return (allValues as unknown[][]).flat() as DecodedArray;
}

/**
 * Read the null mask for a column chunk.
 * Returns a Uint8Array of length `numValues` where 1 = null and 0 = present.
 * Extracts definition levels from DATA_PAGE (V1) and DATA_PAGE_V2 pages.
 * For V1 pages the whole page is decompressed; for V2 pages definition levels
 * are always uncompressed so decompression is skipped.
 */
export async function readNullMask(
  rawBytes: Uint8Array,
  codec: string,
  numValues: number,
): Promise<Uint8Array> {
  const mask = new Uint8Array(numValues); // 0 = present, 1 = null
  const reader = createReader(rawBytes);
  let written = 0;

  while (reader.offset < rawBytes.byteLength && written < numValues) {
    const header = parsePageHeader(reader);
    const pageBytes = new Uint8Array(
      rawBytes.buffer,
      rawBytes.byteOffset + reader.offset,
      header.compressed_page_size,
    );
    reader.offset += header.compressed_page_size;

    if (header.type === "DICTIONARY_PAGE") {
      continue;
    } else if (header.type === "DATA_PAGE") {
      const daph = header.data_page_header!;
      // Decompress page to access definition levels
      const page = await decompressPage(pageBytes, header.uncompressed_page_size, codec);
      const pageReader = createReader(page);
      const defLength = pageReader.view.getUint32(pageReader.offset, true);
      pageReader.offset += 4;
      const levels = new Array<number>(daph.num_values).fill(1);
      if (defLength > 0) {
        readRleBitPackedHybrid(pageReader, 1, levels, defLength);
      }
      for (let i = 0; i < daph.num_values; i++) {
        mask[written++] = levels[i] === 0 ? 1 : 0;
      }
    } else if (header.type === "DATA_PAGE_V2") {
      const daph2 = header.data_page_header_v2!;
      const repLen = daph2.repetition_levels_byte_length;
      const defLen = daph2.definition_levels_byte_length;
      // Definition levels are always uncompressed in V2 pages
      const levels = new Array<number>(daph2.num_values).fill(1);
      if (defLen > 0) {
        const defBytes = pageBytes.subarray(repLen, repLen + defLen);
        const defReader = createReader(defBytes);
        readRleBitPackedHybrid(defReader, 1, levels, defLen);
      }
      for (let i = 0; i < daph2.num_values; i++) {
        mask[written++] = levels[i] === 0 ? 1 : 0;
      }
    }
  }

  return mask;
}
