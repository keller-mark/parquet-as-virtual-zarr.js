# Virtual Zarr dataframe stores — specification

Status: draft, derived from the reference TypeScript implementation in this repository (`src/`).

This document specifies how a Zarr client in any programming language can expose tabular data that is
stored in a non-Zarr format (Parquet, Arrow IPC, CSV/TSV) as a **virtual Zarr v3 hierarchy**, without
first converting the data on disk. A conforming implementation is a read-only key/value store: when a
Zarr library asks it for a key such as `/zarr.json` or `/my_column/c/0`, the store makes up the bytes
that an equivalent on-disk Zarr store would have held for that key.

Four store types are specified:

| Section | Store | Source format | Exposed Zarr layout |
|---|---|---|---|
| [§3](#3-parquetasanndataframestore) | `ParquetAsAnnDataFrameStore` | Parquet file or multi-part Parquet directory | AnnData-Zarr dataframe |
| [§4](#4-arrowasanndataframestore) | `ArrowAsAnnDataFrameStore` | Arrow IPC (file or stream format) | AnnData-Zarr dataframe |
| [§5](#5-csvasanndataframestore) | `CsvAsAnnDataFrameStore` | Delimiter-separated text (CSV, TSV, …) | AnnData-Zarr dataframe |
| [§6](#6-parquetasdataframestore) | `ParquetAsDataFrameStore` | Parquet file or multi-part Parquet directory | `zarr-convention-dataframe` + `zarr-convention-enum` |

§1 and §2 define terms and building blocks that all four stores share. Each store section only lists
what is specific to that store.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be interpreted as described in
RFC 2119.

---

## 1. Conventions and terminology

### 1.1 Store interface

A **store** is a read-only mapping from **keys** to byte strings. Every store in this spec MUST provide
these two operations, using whatever names the host Zarr library expects (for example `get`/`getRange`
in zarrita.js, `get`/`get_partial_values` in zarr-python, `get`/`get_range` in Rust):

- `get(key) → bytes | ABSENT`
- `getRange(key, range) → bytes | ABSENT`, where `range` is one of:
  - `{offset, length}` — return bytes `[offset, offset + length)`;
  - `{suffixLength}` — return the last `suffixLength` bytes.

Keys are absolute, `/`-separated paths starting with `/` (for example `/obs_id/c/0`). `ABSENT` is the
host library's "key not found" value (such as `undefined`, `None`, or a `KeyError`).

Virtual stores MUST implement `getRange(key, range)` so that it gives the same result as computing
`get(key)` and then slicing it. An implementation MAY compute only the requested slice. If `get(key)`
is `ABSENT`, `getRange` MUST also return `ABSENT`.

A store MUST return `ABSENT` for any key that this spec does not route. This includes:

- unknown column names;
- out-of-range chunk indices;
- non-integer chunk indices;
- sub-paths that do not apply to the column's kind (for example `/{numeric_col}/codes/zarr.json`).

### 1.2 Source store

The Parquet-backed stores (§3 and §6) read their input through a **source store**. This is any store
that supports `getRange` on the underlying file or files, for example an HTTP fetch store, a local
filesystem store, or an object-store client. The Arrow (§4) and CSV (§5) stores take the whole input
up front, as bytes or text. A convenience constructor MAY load that input from a source store with
`get("/")`.

### 1.3 Terms

- **Column**: a named one-dimensional field of the source table. Column names that contain `/`, or
  that equal `zarr.json`, are not supported.
- **Row chunk**: a contiguous run of rows that maps to exactly one Zarr chunk. This is a Parquet row
  group (§3), an Arrow record batch (§4), or a fixed-size run of CSV rows (§5).
- **Index column**: the column exposed as the dataframe index (AnnData `_index`).
- **Data columns**: every column other than the index column, in source order.
- **pandas metadata**: the JSON value stored under the key `"pandas"` in a Parquet file's key/value
  metadata or an Arrow schema's custom metadata, as written by pandas and pyarrow. Only these fields
  are used:
  - `index_columns[0]` (a string), and
  - `columns[*].name` together with `columns[*].pandas_type` (where `"categorical"` marks a
    categorical column).

---

## 2. Shared building blocks

### 2.1 Chunk grid and chunk keys

Every row-aligned virtual array (column values, codes, null masks) is one-dimensional and has:

- `shape = [N]`, where `N` is the total number of rows;
- `chunk_grid = {"name": "regular", "configuration": {"chunk_shape": [C]}}`, where `C` is the number
  of rows in row chunk 0;
- `chunk_key_encoding = {"name": "default", "configuration": {"separator": "/"}}`.

So chunk `k` of the array at path `P` has the key `/P/c/k`, and it holds the rows of row chunk `k`.

**Regularity precondition.** This mapping is only correct if every row chunk except the last has
exactly `C` rows, and the last has between 1 and `C` rows. If the source does not meet this
precondition, an implementation MUST raise an error when it opens the source. It MUST NOT silently
serve data with misaligned rows. An implementation MAY instead offer a re-chunking fallback that
gives up the one-to-one mapping.

The last chunk MAY be returned with fewer than `C` elements, holding only the rows that exist. Zarr
clients MUST accept a short final chunk. (The reference implementation and zarrita.js do this. Clients
that require full-size chunks would need the store to pad the final chunk with the `fill_value`.)

### 2.2 Data types

The Zarr v3 `data_type` values used are `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`,
`uint64`, `float32`, `float64`, `bool`, and `string`.

The store returns chunk bytes that are already in the form the declared codec pipeline expects to
decode.

**Numeric (bytes codec).** Numeric chunks are packed little-endian arrays with no header and the
element width of the `data_type`:

- the codec list is `[{"name": "bytes", "configuration": {"endian": "little"}}]`;
- for one-byte types (`int8`, `uint8`, `bool`) it is `[{"name": "bytes"}]`. The reference
  implementation still writes `endian` for `bool`; either form MUST be accepted.

**Boolean.** `bool` chunks hold one byte per element: `0x00` for false and `0x01` for true.

**String (vlen-utf8).** String chunks use `codecs = [{"name": "vlen-utf8"}]` and this byte layout:

```
u32le  item_count
repeat item_count times:
  u32le  byte_length
  u8[byte_length]  UTF-8 bytes of the item
```

Missing string values (nulls) are encoded as the empty string.

**Fill values.** The `fill_value` is `0` for numeric types, `false` for `bool`, and `""` for `string`.

### 2.3 Category ordering and code width

Several stores make an AnnData categorical (`codes` + `categories`) from a column of strings. Two rules
apply.

**Categories.** The categories are the **distinct values** of the column, **sorted ascending by
UTF-16 code-unit order**. This is the default string sort in JavaScript, Java, and C#. In languages
that sort by code point (Python, Rust, Go), implementations MUST use a UTF-16-aware comparator if the
column can contain characters outside the Basic Multilingual Plane. Otherwise their output differs
from the reference.

**Codes.** `codes[i]` is the position of row `i`'s value in the categories array. The data type of
`codes` depends on the number of categories `K`:

| `K` | `codes` data_type | element bytes |
|---|---|---|
| `K ≤ 128` | `int8` | 1 |
| `128 < K ≤ 32768` | `int16` | 2 |
| `K > 32768` | `int32` | 4 |

(Some stores override this table with the source's own index width. Those cases are noted where they
apply.)

### 2.4 AnnData-Zarr dataframe layout

The stores in §3–§5 expose the [AnnData-Zarr dataframe encoding](https://anndata.readthedocs.io/en/latest/fileformat-prose.html#dataframes)
in Zarr v3. All metadata documents are JSON objects, returned as UTF-8 bytes. JSON key order is not
significant. The documents below are templates: `N`, `C`, `K` and the angle-bracketed values are
filled in by each store.

#### 2.4.1 Root group — key `/zarr.json`

```json
{
  "zarr_format": 3,
  "node_type": "group",
  "attributes": {
    "_index": "<index column name>",
    "column-order": ["<data column 1>", "<data column 2>", "..."],
    "encoding-type": "dataframe",
    "encoding-version": "0.2.0"
  }
}
```

`column-order` lists the data columns (it does not include the index column) in source order.

#### 2.4.2 Column kinds

Each column is exactly one of the kinds below. Each store defines how its columns are sorted into
kinds.

| Kind | Node at `/{col}` | Members |
|---|---|---|
| **numeric** | array (§2.4.3) | — |
| **bool** (§3 only) | array (§2.4.4a) | — |
| **string** | array (§2.4.4) | — |
| **categorical** | group (§2.4.5) | `codes` (§2.4.6), `categories` (§2.4.7) |
| **nullable** (§3 only) | group (§2.4.8) | `values`, `mask` (§2.4.9) |

The index column is always exposed as a **string** array, whatever its source type. Its chunk values
MUST be the canonical decimal string form of the source values (for example `10` becomes `"10"`),
matching what AnnData writes for a non-string index.

#### 2.4.3 Numeric column array — `/{col}/zarr.json`

```json
{
  "zarr_format": 3,
  "node_type": "array",
  "shape": [N],
  "data_type": "<numeric data_type>",
  "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [C]}},
  "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
  "fill_value": 0,
  "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
  "attributes": {"encoding-type": "array", "encoding-version": "0.2.0"}
}
```

A store MAY add one bytes-to-bytes compression codec after `bytes` (see §3.6).

#### 2.4.4 String column array — `/{col}/zarr.json`

This document has the same structure as §2.4.3, with these differences:

- `"data_type": "string"`
- `"fill_value": ""`
- `"codecs": [{"name": "vlen-utf8"}]`
- `"attributes": {"encoding-type": "string-array", "encoding-version": "0.2.0"}`

#### 2.4.4a Bool column array — `/{col}/zarr.json`

This document has the same structure as §2.4.3, with these differences:

- `"data_type": "bool"`
- `"fill_value": false`

Its `attributes` stay `{"encoding-type": "array", "encoding-version": "0.2.0"}`. Chunks hold one byte
per row (§2.2).

#### 2.4.5 Categorical group — `/{col}/zarr.json`

```json
{
  "zarr_format": 3,
  "node_type": "group",
  "attributes": {"ordered": false, "encoding-type": "categorical", "encoding-version": "0.2.0"}
}
```

#### 2.4.6 Codes array — `/{col}/codes/zarr.json`

This document has the same structure as §2.4.3, with these differences:

- `data_type` comes from §2.3, or from a store-specific override;
- `attributes` is `{}`.

Chunk `k` holds the codes for row chunk `k`. Rows whose value is missing MUST have code `-1`, the
AnnData missing-category sentinel. Missing values are never categories.

#### 2.4.7 Categories array — `/{col}/categories/zarr.json`

This is a string array with these properties:

- `shape = [K]`;
- `chunk_shape = [K]`;
- `attributes = {"encoding-type": "string-array", "encoding-version": "0.2.0"}`.

It has exactly one chunk, with the key `/{col}/categories/c/0`.

#### 2.4.8 Nullable group — `/{col}/zarr.json`

This follows the [AnnData nullable array specifications](https://github.com/scverse/anndata/blob/main/docs/fileformat-prose.md#nullable-integers-booleans-and-strings)
(v0.1.0). There are three encodings:

| `encoding-type` | `values` member |
|---|---|
| `nullable-integer` | integer array (§2.4.3) |
| `nullable-boolean` | bool array (§2.4.4a) |
| `nullable-string-array` | string array (§2.4.4) |

```json
{
  "zarr_format": 3,
  "node_type": "group",
  "attributes": {"encoding-type": "<nullable-integer | nullable-boolean | nullable-string-array>", "encoding-version": "0.1.0"}
}
```

For `nullable-string-array`, the attributes also include `"na-value": "NA"`. Parquet and Arrow nulls
compare as missing, which is AnnData's `"NA"` missing-value semantics, not `"NaN"`.

AnnData defines **no nullable float encoding**, and anndata itself cannot write one. Float columns with
missing values are therefore plain float arrays (§2.4.3) with `NaN` at the missing rows, which is how
AnnData stores them.

#### 2.4.9 Nullable members

- `/{col}/values/zarr.json`: the `values` array from the table in §2.4.8, including its
  `encoding-type` attributes (`array`, or `string-array` for strings).
- `/{col}/mask/zarr.json`: a bool array (§2.4.4a), so its attributes are
  `{"encoding-type": "array", "encoding-version": "0.2.0"}`.

  Mask byte `1` (true) means the row is **missing**. `0` means the row is present.
- `values` and `mask` have the same `shape` and `chunk_grid` (§2.1).
- In `values`, the slot of a missing row holds a placeholder, matching what anndata writes:
  - `0` for integers;
  - `false` (byte `0`) for booleans;
  - `""` for strings.

### 2.5 Consolidated metadata (optional helper)

An implementation MAY expose a helper, called `getConsolidatedMetadata()` in the reference
implementation. It returns the root group document from §2.4.1 or §6.3 with an added Zarr v3 inline
consolidated-metadata block:

```json
"consolidated_metadata": {"kind": "inline", "must_understand": false, "metadata": { "<path>": <node doc>, "..." }}
```

The `metadata` object follows these rules:

- It has one entry for every node in the hierarchy except the root.
- Each entry is keyed by its path relative to the root, with no leading `/` (for example `obs_id`,
  `cell_type/codes`).
- Child groups (categorical or nullable) carry their own
  `"consolidated_metadata": {"kind": "inline", "must_understand": false, "metadata": {}}`.
  This nested block is empty, because their children are listed in the root's `metadata` object.

`get("/zarr.json")` returns the root document **without** `consolidated_metadata`.

### 2.6 Laziness and caching

Stores SHOULD compute metadata and categories lazily and cache them. Building categories can mean
scanning a whole column (see §3.7.4), so a store MUST NOT build categories until a key under that
column's `codes` or `categories` path is requested, or until the consolidated-metadata helper is
called.

---

## 3. `ParquetAsAnnDataFrameStore`

This store exposes one Parquet file, or a directory of Parquet part files, as an AnnData-Zarr
dataframe (§2.4). Each column chunk inside a row group becomes one Zarr chunk. The store reads the
source **only** with range requests, and it MUST NOT read the whole file.

### 3.1 Inputs

The only input is a source store (§1.2) that supports `getRange`. If the source store has no
`getRange`, the store MUST fail when it is constructed or opened.

### 3.2 Discovering the source (initialization)

Initialization happens once, on the first `get` or `getRange`, and its result is cached.

1. **Single-file mode.** Read the footer (§3.2.1) at source key `/`. If that succeeds, the source has
   one part, at key `/`.
2. **Multi-part mode.** Otherwise, for `n = 0, 1, 2, …`, read the footer at source key
   `/part.{n}.parquet`. Stop at the first `n` where the read fails. The parts are, in order, the `n`
   footers that succeeded.
3. If there are no parts, initialization MUST fail with an error.

All parts MUST have the same schema. The schema, the pandas metadata, and the column classification
come from part 0.

#### 3.2.1 Footer read

1. `getRange(key, {suffixLength: 8})`. If this throws or returns `ABSENT`, the footer read fails.
2. Check that bytes 4–7 equal the ASCII magic `PAR1` (as a u32le, `0x31524150`). If they do not, the
   footer read fails.
3. `metadata_length = u32le(bytes[0..4])`.
4. `getRange(key, {suffixLength: metadata_length + 8})`, and decode the Thrift-compact
   `FileMetaData` from the first `metadata_length` bytes.

An implementation MAY replace steps 1 and 4 with one speculative suffix read (for example the last
64 KiB), plus a second read only if the footer is larger than that.

### 3.3 Global row-group map

Concatenate the row groups of all parts in order, giving global row-group indices `0 … R-1`. For each
global index `g`, record:

- the part, and
- the row-group index local to that part.

These follow:

- `N` = the sum of `num_rows` over all row groups;
- `C` = `num_rows` of global row group 0.

The regularity precondition in §2.1 applies across part boundaries as well.

### 3.4 Column classification

**Columns.** The columns are the schema elements that have a physical `type`, i.e. the leaf columns,
in schema order. Nested (repeated or group) columns are out of scope.

**Index column.** The index column is pandas `index_columns[0]` if it is present. Otherwise it is the
first leaf column.

**Categorical set.** The categorical set is the set of columns that pandas metadata marks
`pandas_type == "categorical"`. Without pandas metadata, the set is empty.

**Value type.** Each column's value type follows from its physical type:

| Parquet physical type | numeric `data_type` |
|---|---|
| `INT32` | `int32` |
| `INT64` | `int64` |
| `FLOAT` | `float32` |
| `DOUBLE` | `float64` |
| `BOOLEAN` | `bool` |
| anything else (`BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`, `INT96`) | — (string) |

Logical/converted types (such as `INT_8`, `DATE`, `TIMESTAMP_*`, `DECIMAL`) are ignored. The physical
type alone decides the mapping.

**Kind.** Each column gets the first kind in this list that applies to it:

1. In the categorical set → **categorical**.
2. Is the index column → **string**.
3. `repetition_type == OPTIONAL` and the physical type is not `FLOAT` or `DOUBLE` → **nullable**.
   The encoding-type depends on the physical type:
   - `BOOLEAN` → `nullable-boolean`;
   - `INT32` / `INT64` → `nullable-integer`;
   - any other type → `nullable-string-array`.
4. Numeric physical type → **numeric**. This includes `OPTIONAL FLOAT` / `DOUBLE` columns, whose
   missing values become `NaN`. AnnData has no nullable float encoding (§2.4.8) and stores float
   columns with missing values the same way.
5. `BOOLEAN` (necessarily `REQUIRED` here) → **bool**.
6. Anything else → **string**.

**Regularity.** After building the row-group map (§3.3), the store MUST check the §2.1 precondition
and fail to open if it does not hold.

### 3.5 Key routing

| Key | Condition | Result |
|---|---|---|
| `/zarr.json` | — | Root group (§2.4.1) |
| `/{col}/zarr.json` | numeric | §2.4.3 (codecs per §3.6) |
| `/{col}/zarr.json` | bool | §2.4.4a |
| `/{col}/zarr.json` | string | §2.4.4 |
| `/{col}/zarr.json` | categorical | §2.4.5 |
| `/{col}/zarr.json` | nullable | §2.4.8 |
| `/{col}/c/{g}` | numeric, bool or string, `0 ≤ g < R` | §3.7.1 |
| `/{col}/values/zarr.json` | nullable | §2.4.9 |
| `/{col}/values/c/{g}` | nullable, `0 ≤ g < R` | §3.7.2 |
| `/{col}/mask/zarr.json` | nullable | §2.4.9 |
| `/{col}/mask/c/{g}` | nullable, `0 ≤ g < R` | §3.7.3 |
| `/{col}/codes/zarr.json` | categorical | §2.4.6 |
| `/{col}/codes/c/{g}` | categorical, `0 ≤ g < R` | §3.7.4 |
| `/{col}/categories/zarr.json` | categorical | §2.4.7 |
| `/{col}/categories/c/0` | categorical | §3.7.4 |
| anything else | — | `ABSENT` |

Only `/zarr.json` and the `zarr.json` keys of non-categorical columns can be served from the footer
alone. Keys
under `codes` and `categories` need the category set (§3.7.4).

### 3.6 Zero-copy compressed pass-through

A **numeric** column is **zero-copy eligible** if all of these hold in **every** row group of
**every** part:

- its physical type is `INT32`, `INT64`, `FLOAT` or `DOUBLE`;
- its `repetition_type` is `REQUIRED`;
- its column-chunk `encodings` contain neither `RLE_DICTIONARY` nor `PLAIN_DICTIONARY`;
- its column-chunk `codec` is the same everywhere;
- if that codec is not `UNCOMPRESSED`, the column chunk's `encoding_stats` are present, list no
  `DICTIONARY_PAGE`, and count exactly one data page (`DATA_PAGE` plus `DATA_PAGE_V2`).

The last condition exists because separately compressed page payloads cannot, in general, be
concatenated into one valid compressed stream. Without `encoding_stats` the page count is unknown
from the footer, so the column is not eligible. An implementation MAY instead learn page counts from
the Parquet `OffsetIndex`, at the cost of extra reads.

All of these conditions are checked against footer metadata only, at no I/O cost.

For an eligible column, the Parquet column-chunk `codec` decides the virtual codec pipeline:

| Parquet codec | Virtual `codecs` |
|---|---|
| `UNCOMPRESSED` | `[bytes]` |
| `SNAPPY` | `[bytes, {"name": "snappy"}]` |
| `GZIP` | `[bytes, {"name": "gzip"}]` |
| `ZSTD` | `[bytes, {"name": "zstd"}]` |
| `LZ4_RAW` | `[bytes, {"name": "lz4_raw"}]` |
| `BROTLI` | `[bytes, {"name": "brotli"}]` |
| other (`LZO`, legacy `LZ4`) | not eligible: decode path (§3.7.1) and `[bytes]` |

The compression codecs carry no `configuration`. If the host Zarr library requires one (for example
some `gzip` or `zstd` implementations require `level`), the implementation MAY add a dummy value,
because decoding ignores it.

A column that is not eligible uses the decode path (§3.7.1) and `[bytes]` for all row groups.

#### 3.6.1 Codec definitions

A client that reads these stores MUST register decoders for the codec names below, if its Zarr
library does not already provide them. All of them are decode-only, bytes-to-bytes codecs.

- `snappy`: the raw Snappy block format (not the framed format), as used by Parquet.
- `gzip`: a gzip (RFC 1952) member stream. This is compatible with the standard Zarr v3 `gzip` codec.
- `zstd`: a Zstandard frame. This is compatible with the standard Zarr v3 `zstd` codec.
- `brotli`: a raw Brotli stream.
- `lz4_raw`: `u32le uncompressed_size` followed by one raw LZ4 block, with no LZ4 frame. Decoding
  reads the 4-byte prefix, sets aside a buffer of that size, and decompresses the block into it. (The
  prefix exists because a raw LZ4 block does not record its own output size.)

### 3.7 Producing chunk bytes

#### 3.7.0 Column-chunk byte range

For column `col` and global row group `g`, use the `ColumnMetaData` of the matching column chunk in
the part's local row group. Its byte range is:

- `start = dictionary_page_offset` if that is present and non-zero, otherwise `data_page_offset`;
- `length = total_compressed_size`.

Fetch the range with **one** `getRange(part_key, {offset: start, length})`. The fetched bytes are a
sequence of Thrift-compact `PageHeader`s, each followed by `compressed_page_size` bytes of page
payload. An implementation MUST NOT read any other part of the file to serve a chunk.

#### 3.7.1 Numeric, bool or string column chunk — `/{col}/c/{g}`

**Zero-copy path.** This path applies to zero-copy eligible columns (§3.6).

1. Walk the page headers. If the chunk has exactly one data page (`DATA_PAGE` or `DATA_PAGE_V2`),
   return that page's payload bytes **unchanged** (still compressed with the Parquet codec).
2. For `lz4_raw`, put `u32le(uncompressed_page_size)` from the page header in front of the payload.
3. Otherwise, the output MUST still match the codec pipeline that the metadata declares. This covers
   a chunk with more than one data page, and a `DATA_PAGE_V2` with `is_compressed = false` under a
   compressed codec.
   - If the declared pipeline is `[bytes]` (`UNCOMPRESSED`), take the decode path.
   - If it includes a compression codec, the footer's `encoding_stats` were wrong. The store MUST
     either compress the decoded values with the declared codec or fail the request with an error.
     It MUST NOT return uncompressed bytes.

**Decode path.** This path applies to all other columns.

1. Decode the column chunk into one value per row. Support at least these page types and encodings:
   - `DICTIONARY_PAGE`;
   - `DATA_PAGE` (v1) and `DATA_PAGE_V2`;
   - `PLAIN` encoding;
   - `RLE_DICTIONARY` / `PLAIN_DICTIONARY` encoding;
   - compression with any codec in §3.6.

   For `OPTIONAL` columns, decode the definition levels: an RLE/bit-packed hybrid with bit width 1,
   where 0 means null. Then expand the non-null values back to full length, using the placeholders in
   §2.4.9.
2. Encode the result:
   - numeric: packed little-endian values of the column's `data_type`, with codecs `[bytes]`;
   - bool: one byte per row (§2.2);
   - string: vlen-utf8 (§2.2), decoding `BYTE_ARRAY` values as UTF-8. Non-string values (for
     example a numeric index column) are converted to their decimal string form, and missing values
     to `""`.

#### 3.7.2 Nullable values — `/{col}/values/c/{g}`

Use the decode path of §3.7.1, including null expansion. Encode the result as:

- packed little-endian values for `nullable-integer`;
- vlen-utf8 for `nullable-string-array`;
- one byte per value (0 or 1) for `nullable-boolean`.

The zero-copy path does not apply, because nullable columns are never `REQUIRED`.

#### 3.7.3 Null mask — `/{col}/mask/c/{g}`

Return one byte per row of row group `g`: `1` where the definition level is 0, and `0` otherwise.

- For `DATA_PAGE` (v1), decompress the page and read the definition levels from the start of the
  page (a `u32le` byte length, then the RLE/bit-packed hybrid data).
- For `DATA_PAGE_V2`, the definition levels are stored uncompressed, after the repetition levels.
  Read them **without** decompressing the page.
- Skip dictionary pages.

#### 3.7.4 Categorical codes and categories

**Categories.** Build the categories of column `col` with §2.3, over the decoded values of `col`
across **all** row groups (and all parts). Cache the result.

This is the one operation that reads beyond a single column chunk. It still reads only `col`'s column
chunks, never other columns.

If every column chunk of `col` is fully dictionary-encoded, an implementation MAY build the categories
from the dictionary pages alone. It reads each chunk only up to `data_page_offset`, deduplicates the
union of the dictionaries, and sorts it as in §2.3. The result MUST be the same as a full scan.

**`/{col}/categories/c/0`.** The vlen-utf8 encoding of the categories.

**`/{col}/codes/c/{g}`.**

1. Decode row group `g` of `col`. For `OPTIONAL` columns, use the definition levels to find the rows
   that are missing.
2. Map each value to its position in the categories, and each missing row to `-1`.
3. Pack the codes little-endian at the §2.3 width.

Missing rows are excluded when building the categories.

### 3.8 I/O guarantees

A conforming implementation:

- reads only:
  - footer suffixes (§3.2.1), and
  - whole column-chunk ranges (§3.7.0) for the columns and row groups that are requested,
  apart from the full-column scan allowed when building categories (§3.7.4);
- never issues a `get` (an unbounded read) against the source store;
- caches parsed footers, so each footer is read at most once for each store instance.

---

## 4. `ArrowAsAnnDataFrameStore`

This store exposes an Arrow table, deserialized from Arrow IPC bytes, as an AnnData-Zarr dataframe
(§2.4). Each record batch becomes one Zarr chunk. The whole IPC buffer is held in memory, so there is
no range-request behaviour to specify. Where possible, chunks are served as **views into the Arrow
buffers**, without copying.

### 4.1 Inputs

The input is the bytes of one Arrow IPC payload, in either the file format (`ARROW1` magic) or the
streaming format. All columns MUST be top-level, non-nested fields. All batches share the same row
boundaries across columns, as Arrow record batches always do.

### 4.2 Table geometry

- `N` = the table row count.
- `R` = the number of record batches.
- `C` = the row count of batch 0. §2.1 regularity applies.

### 4.3 Column classification

**Columns.** The columns are the schema fields, in order.

**Index column.** The index column is pandas `index_columns[0]` from the schema's `"pandas"` custom
metadata if that is present. Otherwise it is the first field.

**Categorical set.**

- If pandas metadata lists at least one `categorical` column, the categorical set is exactly the
  columns that pandas metadata marks `categorical`.
- Otherwise, it is every field whose Arrow type is `Dictionary`.

**Value type.**

| Arrow type | `data_type` |
|---|---|
| `Int(8/16/32/64, signed)` | `int8` / `int16` / `int32` / `int64` |
| `Int(8/16/32/64, unsigned)` | `uint8` / `uint16` / `uint32` / `uint64` |
| `FloatingPoint(HALF or SINGLE)` | `float32` |
| `FloatingPoint(DOUBLE)` | `float64` |
| anything else | string |

Half-precision (`HALF`) values MUST be widened to `float32` when chunks are served. Serving the raw
2-byte buffer under a `float32` declaration is incorrect.

**Kind.** Each column gets the first kind that applies: **categorical**, then **string** (for the index
column), then **numeric** (for a numeric type), then **string**. This store never produces the
nullable kind.

### 4.4 Key routing

This store uses the routing table of §3.5, minus the nullable rows. `g` is a batch index, `0 ≤ g < R`.

### 4.5 Producing chunk bytes

- **Numeric `/{col}/c/{g}`.** Return the first `len(batch_g) × element_size` bytes of batch `g`'s
  values buffer, starting at its byte offset. This is a zero-copy view; the Arrow buffer is already
  little-endian. `HALF` columns are the exception: they are widened to `float32` (§4.3). Validity bitmaps are ignored, so the bytes in null slots are whatever the buffer
  holds.
- **String `/{col}/c/{g}`.** Turn each element of batch `g` into a string with the host language's
  canonical conversion (for example decimal for numbers and `true`/`false` for booleans). Use `""` for
  nulls. Encode the result as vlen-utf8.
- **Categorical.** There are two cases, depending on whether the column is **zero-copy-codes
  eligible**.

  A column is zero-copy-codes eligible if all of these hold:
  - its Arrow type is `Dictionary`;
  - every batch refers to the **same** dictionary;
  - that dictionary is non-empty;
  - that dictionary is **strictly ascending** in the §2.3 order, which also means it has no duplicates;
  - no batch has nulls. The index buffer holds arbitrary values at null rows, but those rows need
    code `-1`.

  *Eligible columns:*
  - The categories are the dictionary values in their stored order.
  - `codes` `data_type` is the Arrow dictionary index type: `int8`/`int16`/`int32`/`int64`, or
    `uint8`/`uint16`/`uint32`/`uint64` for unsigned indices. This overrides §2.3.
  - `/{col}/codes/c/{g}` is a zero-copy view of batch `g`'s index buffer.

  *Ineligible columns:*
  - For a `Dictionary` type, the categories are the distinct non-null values of the union of all
    batch dictionaries (batches can carry replacement or delta dictionaries), sorted as in §2.3. For
    any other type, they are the distinct non-null values across all batches, converted to strings
    and sorted the same way.
  - Codes are made by mapping each row's value to its position in the categories, using the batch's
    own dictionary when there is one. Missing rows get `-1`.
  - Codes are packed at the §2.3 width.
- **`/{col}/categories/c/0`.** The categories, encoded as vlen-utf8.

---

## 5. `CsvAsAnnDataFrameStore`

This store exposes delimiter-separated text as an AnnData-Zarr dataframe (§2.4). The text is parsed
in full and held in memory. Chunking is synthetic.

### 5.1 Inputs and options

| Option | Default | Meaning |
|---|---|---|
| `separator` | `","` | One-character field delimiter. Use `"\t"` for TSV. |
| `columnTypes` | `{}` | Per-column override: one of `int8`, `int16`, `int32`, `float32`, `float64`, `string`, `categorical`. |
| `indexColumn` | first header field | Name of the index column. |
| `chunkSize` | `max(1, N)` (one chunk) | Rows per Zarr chunk. Values below 1 are clamped to 1. |

### 5.2 Parsing

The text is parsed as RFC 4180-style delimited data:

- The first record is the header and gives the column names, in order.
- Fields MAY be quoted with `"`, and a doubled `""` inside a quoted field is a literal `"`.
- Records end at `\n` or `\r\n`.
- Every later record is one row. Missing trailing fields read as the empty string.

`N` is the number of data rows.

### 5.3 Type inference

For each column without an override, consider all of its `N` raw string values in turn:

1. **Numeric test.** Skip empty strings. Every non-empty value MUST parse as a finite or infinite
   number. Parsing follows ECMAScript `StringToNumber`: it trims leading and trailing whitespace, and
   accepts decimal, exponent, `Infinity`, and `0x`/`0o`/`0b` literals. Implementations in other
   languages SHOULD at least accept decimal and exponent forms, and MUST document any difference.
   - If every value passes and every parsed value is an integer, the type is `int32`.
   - If every value passes and some parsed value is not an integer, the type is `float64`.
2. **Categorical test.** Otherwise, let `U` be the number of distinct raw values, counting the empty
   string as a value. If `N > 0`, `U / N ≤ 0.5`, and `U ≤ 1000`, the type is `categorical`.
3. Otherwise, the type is `string`.

### 5.4 Kind

Each column gets the first kind that applies:

1. type `categorical` → **categorical**;
2. the index column, or type `string` → **string**;
3. otherwise → **numeric**, with the `data_type` equal to the type name.

### 5.5 Geometry and routing

- `C = chunkSize`.
- `R = ceil(N / C)`.
- Chunk `k` covers rows `[k·C, min((k+1)·C, N))`.

Routing follows §3.5, minus the nullable rows.

### 5.6 Producing chunk bytes

- **Numeric.** Parse each raw value. The empty string becomes `0`. Pack the values little-endian at
  the column's `data_type` width, converting with the host's standard numeric cast. (Integer overflow
  behaviour for overridden narrow types is implementation-defined; the reference wraps.)
- **String** (including a numeric-looking index column). Encode the raw field text, unchanged, as
  vlen-utf8.
- **Categorical.**
  - The categories are the distinct raw values of the column, including `""` if it occurs, sorted as
    in §2.3.
  - Codes use the §2.3 width.
  - The empty string is an ordinary category, not a missing value.

---

## 6. `ParquetAsDataFrameStore`

This store exposes a Parquet source as a dataframe that follows
[`zarr-convention-dataframe` v1](https://github.com/scverse/zarr-convention-dataframe/blob/v1/README.md).
Its categorical columns follow [`zarr-convention-enum` v1](https://github.com/scverse/zarr-convention-enum/blob/v1/README.md).

It is specified as a **key-remapping view** over an inner AnnData-dataframe store `A`. In the reference
implementation, `A` is a §3 store built from the same source store. Every chunk byte and every array
metadata field except `attributes` is taken unchanged from `A`. This makes all of §3's I/O guarantees
carry over.

The construction applies just as well to the §4 and §5 stores. An implementation MAY offer it
generically, as a "dataframe-convention view of any AnnData-dataframe store".

### 6.1 Convention descriptors

```json
DF_CONVENTION = {
  "uuid": "1f1b36d2-3185-6da8-91f6-9a0823af2d33",
  "schema_url": "https://raw.githubusercontent.com/scverse/zarr-convention-dataframe/refs/tags/v1/schema.json",
  "spec_url": "https://github.com/scverse/zarr-convention-dataframe/blob/v1/README.md",
  "name": "df:",
  "description": "A collection of 1d-interpreted arrays to be used as a dataframe"
}

ENUM_CONVENTION = {
  "uuid": "c906c423-56ed-413c-9943-7b2ff52d18f2",
  "schema_url": "https://raw.githubusercontent.com/scverse/zarr-convention-enum/refs/tags/v1/schema.json",
  "spec_url": "https://github.com/scverse/zarr-convention-enum/blob/v1/README.md",
  "name": "enum:",
  "description": "Dictionary-encoded enum array: integer codes indexing into a values array."
}
```

### 6.2 Layout (computed once, cached)

1. Read `A`'s `/zarr.json`, and let `idx = attributes._index` and `order = attributes["column-order"]`.
2. `columns = [idx, ...order]`. Column `i` is exposed as the member `column_{i}`, so the index is
   always `column_0`.
3. For each `columns[i]`, read `A`'s `/{columns[i]}/zarr.json` and set the column's kind:
   - `node_type == "array"` → **array**;
   - a group with `encoding-type == "categorical"` → **enum**;
   - any other group (AnnData nullable) → **group**.
4. `numRows = shape[0]` of `A`'s `/{idx}/zarr.json`.

### 6.3 Root group — `/zarr.json`

```json
{
  "zarr_format": 3,
  "node_type": "group",
  "attributes": {
    "zarr_conventions": [DF_CONVENTION],
    "df:shape": [numRows, len(columns)],
    "df:index": 0
  }
}
```

The root carries none of the AnnData attributes (`_index`, `column-order`, `encoding-*`).

### 6.4 Column-name array — `/columns`

- `/columns/zarr.json` is a string array with these properties:
  - `shape = [len(columns)]`;
  - `chunk_shape = [max(len(columns), 1)]`;
  - `"codecs": [{"name": "vlen-utf8"}]`;
  - `"fill_value": ""`;
  - `"attributes": {}`.
- `/columns/c/0` is the vlen-utf8 encoding of `columns`.
- Any other key under `/columns/` is `ABSENT`.

### 6.5 Column members — `/column_{i}/…`

The member name MUST match `^column_(0|[1-9][0-9]*)$`, with `i < len(columns)`. Zero-padded or
out-of-range names are `ABSENT`. Let `col = columns[i]` and let `rest` be the remainder of the key.

**Attribute stripping.** For any **array** metadata document taken from `A`, remove the
`encoding-type` and `encoding-version` attributes, and keep every other field. Group documents are not
stripped, so nullable groups keep their AnnData `encoding-type`.

**Routing:**

| `rest` | kind | Result |
|---|---|---|
| `zarr.json` | enum | Enum group (below) |
| `zarr.json` | array or group | `A[/{col}/zarr.json]`, attribute-stripped |
| `categories/…` | enum | `ABSENT`. The AnnData name is hidden. |
| `enum_values/{tail}` | enum | Same as `A[/{col}/categories/{tail}]` |
| `…/zarr.json` (any other) | any | `A[/{col}/{rest}]`, attribute-stripped; `ABSENT` if `A` returns `ABSENT` |
| anything else (chunk keys) | any | `A[/{col}/{rest}]`, passed through byte-for-byte |

**Enum group document:**

```json
{
  "zarr_format": 3,
  "node_type": "group",
  "attributes": {
    "zarr_conventions": [ENUM_CONVENTION],
    "enum:ordered": <A's categorical "ordered" attribute>,
    "enum:codes": ["codes"]
  }
}
```

So an enum column has two members:

- `codes`, which is `A`'s `codes` array, and
- `enum_values`, which is `A`'s `categories` array (attribute-stripped).

A nullable column has the members `values` and `mask`, unchanged apart from attribute stripping.

### 6.6 Consolidated metadata

If an implementation provides the helper from §2.5, it builds it as follows:

1. Start from `A`'s consolidated metadata.
2. Rename every entry key that starts with `col` (meaning `col` itself or `col/…`):
   - `col` → `column_{i}`;
   - `col/categories` → `column_{i}/enum_values` for enum columns;
   - `col/…` → `column_{i}/…` otherwise.
3. Replace the enum group entries with the enum group document from §6.5, keeping their nested
   `consolidated_metadata` block.
4. Apply attribute stripping to the array entries.
5. Add a `columns` entry, holding the document from §6.4.
6. Attach the result to the root document from §6.3.

---

## Appendix A. Conformance testing

An implementation is conformant if, for each source fixture, opening the virtual store with a Zarr v3
reader gives the same **structure** and **values** as opening the equivalent dataframe written by
`anndata` (for §3–§5) or by a convention-aware writer (for §6). Specifically, the following must match:

- group and array membership;
- `shape` and `data_type`;
- the relevant attributes;
- decoded array contents.

Byte-for-byte equality of metadata or chunks is **not** required. Chunking and compression usually
differ from the reference Zarr fixture; that is the point of the virtual mapping.

Implementations SHOULD also check these I/O properties:

- (§3) Reading one chunk issues exactly one `getRange` against the source store, after initialization.
- (§3) No request reads the whole source file.
- (§3) Zero-copy chunks are byte-identical to the Parquet page payload.
