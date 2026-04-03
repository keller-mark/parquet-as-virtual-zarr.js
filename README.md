# parquet-as-virtual-zarr.js

Read Parquet data via the Zarr store interface.
Given a Parquet file, we want to conceptually/virtually map its columns and row-groups to Zarr chunks.

To do so, we rely on the AnnData-Zarr [on-disk dataframe format](https://anndata.readthedocs.io/en/latest/fileformat-prose.html#dataframes) (see the "Zarr" tabs).

## Usage

```ts
import { open, get, root } from "zarrita";
import { FetchStore } from "@zarrita/storage";
import { ParquetAsAnnDataFrameStore } from "parquet-as-virtual-zarr";

const internalStore = new FetchStore("https://example.com/data.parquet");
const store = ParquetAsAnnDataFrameStore.fromStore(source);

const storeRoot = root(store);
const df = await open(storeRoot, { kind: "group" });
console.log(df.attrs);

const arr = await open(storeRoot.resolve("/my_column"), { kind: "array" });
console.log(arr.attrs);
const arrData = await get(arr);
```

## Virtual arrow-to-zarr mapping

### `/.zattrs` (Zarr v2) or `/zarr.json` (Zarr v3) attrs for root of dataframe

This should list the column names in the dataframe.

### Column mapping `/{colname}`

#### Column attrs `/{colname}/.zattrs`

This should list the `dtype` of the column and other properties of the column array, such as whether it uses dictionary encoding (categories+codes).

#### Column row group `/{colname}/{row_group_index}`

For numeric (and other non-dictionary) columns, we lookup each row group using the row group index in the Zarr key path.

For instance, if your Zarr store key was `/cell_type/0`, then this corresponds to the `cell_type` column of the Arrow table and the `0`th row group.

-----

### Related work:
- https://github.com/zarr-developers/zarr-extensions/pull/41
- https://github.com/srivarra/zarrwhals
- https://github.com/hypertidy/zaro
- https://github.com/keller-mark/hdf5-as-virtual-zarr.js
- https://github.com/keller-mark/tiff-as-virtual-zarr.js

Note: This is not an effort to read [Zarr as Arrow](https://github.com/datafusion-contrib/arrow-zarr/issues/36) (the reverse).
