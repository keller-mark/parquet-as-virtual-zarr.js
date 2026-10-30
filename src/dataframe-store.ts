import type { AsyncReadable, AbsolutePath, RangeQuery } from "@zarrita/storage";
import { ParquetAsAnnDataFrameStore } from "./parquet-store.js";

const DF_CONVENTION = {
  uuid: "1f1b36d2-3185-6da8-91f6-9a0823af2d33",
  schema_url: "https://raw.githubusercontent.com/scverse/zarr-convention-dataframe/refs/tags/v1/schema.json",
  spec_url: "https://github.com/scverse/zarr-convention-dataframe/blob/v1/README.md",
  name: "df:",
  description: "A collection of 1d-interpreted arrays to be used as a dataframe",
};

const ENUM_CONVENTION = {
  uuid: "c906c423-56ed-413c-9943-7b2ff52d18f2",
  schema_url: "https://raw.githubusercontent.com/scverse/zarr-convention-enum/refs/tags/v1/schema.json",
  spec_url: "https://github.com/scverse/zarr-convention-enum/blob/v1/README.md",
  name: "enum:",
  description: "Dictionary-encoded enum array: integer codes indexing into a values array.",
};

/**
 * How a column is laid out:
 *   array    → plain 1d array
 *   enum     → zarr-convention-enum group (codes + enum_values)
 *   group    → other 1d-interpreted group (e.g. AnnData nullable values + mask)
 */
type ColumnKind = "array" | "enum" | "group";

interface Layout {
  /** Column names in column order; column i is stored at member `column_{i}`. */
  columns: string[];
  kinds: ColumnKind[];
  /** Position of the index column within `columns`. */
  index: number;
  numRows: number;
}

/**
 * Virtual Zarr v3 store that exposes a Parquet file (or multi-part parquet
 * directory) as a dataframe conforming to the zarr-convention-dataframe spec
 * (https://github.com/scverse/zarr-convention-dataframe).
 *
 * Categorical columns are exposed using the zarr-convention-enum spec
 * (https://github.com/scverse/zarr-convention-enum).
 *
 * This store re-maps keys onto a ParquetAsAnnDataFrameStore, which handles
 * reading row groups from the Parquet file via range requests.
 *
 * Key routing (Zarr v3):
 *   /zarr.json                        → dataframe group metadata (df:shape, df:index)
 *   /columns/zarr.json                → column names array metadata
 *   /columns/c/0                      → column names chunk bytes (vlen-utf8)
 *   /column_{i}/zarr.json             → array metadata, or group metadata (enum / nullable)
 *   /column_{i}/c/{rg}                → raw chunk bytes (plain columns)
 *   /column_{i}/codes/…               → codes array (enum columns)
 *   /column_{i}/enum_values/…         → distinct values array (enum columns)
 *   /column_{i}/values/…, mask/…      → nullable column sub-arrays
 *
 * The index column is always `column_0`, followed by the remaining columns.
 */
export class ParquetAsDataFrameStore implements AsyncReadable {
  readonly #inner: ParquetAsAnnDataFrameStore;
  #layout: Promise<Layout> | null = null;

  constructor(store: AsyncReadable) {
    this.#inner = new ParquetAsAnnDataFrameStore(store);
  }

  static fromStore(store: AsyncReadable): ParquetAsDataFrameStore {
    return new ParquetAsDataFrameStore(store);
  }

  async #innerJson(key: string): Promise<Record<string, any> | undefined> {
    const bytes = await this.#inner.get(key as AbsolutePath);
    if (!bytes) return undefined;
    return JSON.parse(new TextDecoder().decode(bytes));
  }

  #getLayout(): Promise<Layout> {
    this.#layout ??= (async () => {
      const rootAttrs = (await this.#innerJson("/zarr.json"))!.attributes;
      const indexName: string = rootAttrs._index;
      const columns: string[] = [indexName, ...rootAttrs["column-order"]];
      const kinds: ColumnKind[] = [];
      for (const col of columns) {
        const meta = (await this.#innerJson(`/${col}/zarr.json`))!;
        if (meta.node_type === "array") kinds.push("array");
        else if (meta.attributes?.["encoding-type"] === "categorical") kinds.push("enum");
        else kinds.push("group");
      }
      // The AnnData index is always a plain (non-nullable, non-categorical) array.
      const indexMeta = (await this.#innerJson(`/${indexName}/zarr.json`))!;
      return { columns, kinds, index: 0, numRows: indexMeta.shape[0] };
    })();
    return this.#layout;
  }

  // ── Zarr v3 metadata helpers ─────────────────────────────────────────────

  #rootGroupMeta(layout: Layout): Record<string, unknown> {
    return {
      zarr_format: 3,
      node_type: "group",
      attributes: {
        zarr_conventions: [DF_CONVENTION],
        "df:shape": [layout.numRows, layout.columns.length],
        "df:index": layout.index,
      },
    };
  }

  #columnsArrayMeta(layout: Layout): Record<string, unknown> {
    const n = layout.columns.length;
    return {
      zarr_format: 3,
      node_type: "array",
      shape: [n],
      data_type: "string",
      chunk_grid: { name: "regular", configuration: { chunk_shape: [Math.max(n, 1)] } },
      chunk_key_encoding: { name: "default", configuration: { separator: "/" } },
      fill_value: "",
      codecs: [{ name: "vlen-utf8" }],
      attributes: {},
    };
  }

  #enumGroupMeta(ordered: boolean): Record<string, unknown> {
    return {
      zarr_format: 3,
      node_type: "group",
      attributes: {
        zarr_conventions: [ENUM_CONVENTION],
        "enum:ordered": ordered,
        "enum:codes": ["codes"],
      },
    };
  }

  /** Remove AnnData-specific encoding attributes from array metadata. */
  #stripAnnDataAttrs(meta: Record<string, any>): Record<string, any> {
    if (meta.node_type !== "array") return meta;
    const { "encoding-type": _t, "encoding-version": _v, ...attributes } = meta.attributes ?? {};
    return { ...meta, attributes };
  }

  #encodeVlenUtf8(strings: string[]): Uint8Array {
    const encoder = new TextEncoder();
    const encoded = strings.map((s) => encoder.encode(s));
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

  /**
   * Map a path within `column_{i}` (e.g. "enum_values/c/0") to the
   * corresponding path within the AnnData column (e.g. "categories/c/0").
   */
  #innerSubpath(kind: ColumnKind, subpath: string): string {
    if (kind === "enum" && (subpath === "enum_values" || subpath.startsWith("enum_values/"))) {
      return "categories" + subpath.slice("enum_values".length);
    }
    return subpath;
  }

  /** Parse `column_{i}` into i, rejecting leading zeros and out-of-range values. */
  #columnIndex(member: string, layout: Layout): number | undefined {
    const match = /^column_(0|[1-9][0-9]*)$/.exec(member);
    if (!match) return undefined;
    const i = Number(match[1]);
    return i < layout.columns.length ? i : undefined;
  }

  /**
   * Build virtual consolidated metadata matching zarr v3 format.
   * Returns the full root zarr.json with inline consolidated_metadata.
   */
  async getConsolidatedMetadata(): Promise<Record<string, unknown>> {
    const layout = await this.#getLayout();
    const innerConsolidated = await this.#inner.getConsolidatedMetadata();
    const innerMeta = (innerConsolidated.consolidated_metadata as any).metadata as Record<string, any>;
    const metadata: Record<string, unknown> = {
      columns: this.#columnsArrayMeta(layout),
    };
    layout.columns.forEach((col, i) => {
      const kind = layout.kinds[i];
      for (const [innerKey, meta] of Object.entries(innerMeta)) {
        if (innerKey === col) {
          metadata[`column_${i}`] = kind === "enum"
            ? { ...this.#enumGroupMeta(meta.attributes.ordered), consolidated_metadata: meta.consolidated_metadata }
            : this.#stripAnnDataAttrs(meta);
        } else if (innerKey.startsWith(`${col}/`)) {
          let subpath = innerKey.slice(col.length + 1);
          if (kind === "enum" && subpath === "categories") subpath = "enum_values";
          metadata[`column_${i}/${subpath}`] = this.#stripAnnDataAttrs(meta);
        }
      }
    });
    return {
      ...this.#rootGroupMeta(layout),
      consolidated_metadata: { kind: "inline", must_understand: false, metadata },
    };
  }

  async get(key: AbsolutePath, _opts?: unknown): Promise<Uint8Array | undefined> {
    const layout = await this.#getLayout();
    return this.#route(key, layout);
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

  async #route(key: string, layout: Layout): Promise<Uint8Array | undefined> {
    if (key === "/zarr.json") {
      return this.#json(this.#rootGroupMeta(layout));
    }

    const parts = key.split("/").filter(Boolean);
    if (parts.length < 2) return undefined;

    // /columns/…
    if (parts[0] === "columns") {
      if (parts.length === 2 && parts[1] === "zarr.json") {
        return this.#json(this.#columnsArrayMeta(layout));
      }
      if (parts.length === 3 && parts[1] === "c" && parts[2] === "0") {
        return this.#encodeVlenUtf8(layout.columns);
      }
      return undefined;
    }

    // /column_{i}/…
    const i = this.#columnIndex(parts[0], layout);
    if (i === undefined) return undefined;
    const col = layout.columns[i];
    const kind = layout.kinds[i];
    const subpath = parts.slice(1).join("/");

    if (subpath === "zarr.json") {
      const meta = (await this.#innerJson(`/${col}/zarr.json`))!;
      if (kind === "enum") return this.#json(this.#enumGroupMeta(meta.attributes.ordered));
      return this.#json(this.#stripAnnDataAttrs(meta));
    }

    // Prevent reaching AnnData's "categories" member under its original name.
    if (kind === "enum" && parts[1] === "categories") return undefined;

    const innerKey = `/${col}/${this.#innerSubpath(kind, subpath)}` as AbsolutePath;
    if (parts[parts.length - 1] === "zarr.json") {
      const meta = await this.#innerJson(innerKey);
      return meta && this.#json(this.#stripAnnDataAttrs(meta));
    }
    return this.#inner.get(innerKey);
  }
}
