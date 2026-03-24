
## Submodules

This repository contains the following as git submodules, for your reference:

- zarrita.js: Zarr implementation in TypeScript/JavaScript.
- hyparquet
- flechette



## Project goal

Read Parquet files as virtual Zarr-based dataframes (conforming to the dataframe part of the AnnData-Zarr on-disk file format spec) — without copying or re-encoding the Arrow data.
Columns within row groups are fetched directly as Zarr chunks by mapping Zarr chunk key requests to byte ranges within the Parquet file.

ParquetAsAnnDataFrameZarr should be defined in the NPM package and exported. This class must implement zarrita's
AsyncReadable interface. Given the virtualization convention defined in the README, this store should enable a user to run .get and
 .getRange to obtain the store's metadata or array data. Internally, the store should fetch the Parquet data for the
corresponding metadata or array chunks. Metadata should be transformed and re-encoded to match what would be
returned when running .get or .getRange on the equivalent AnnData-Zarr dataframe. Unit tests should ensure this equivalence of metadata and
array data, so that the user can ultimately read their Parquet data as if it were a "virtualized" AnnData dataframe. The user should not be able to
 tell that the underlying data on-disk is not in AnnData-Zarr dataframe format.

In unit tests, comparisons of data and metadata do not need to be byte-for-byte, they can structurally compare the final object/array representation. Also, the store will need to
virtually return the zarr array metadata that corresponds to the underlying Parquet row-group shape/chunking.

Although the test fixtures are small, we want this store to support very large Parquet files, so
getRange will be used to read the underlying Parquet file using range requests. We always want to read the minimal subset of bytes of
 the file that are necessary, and we never want to read the full file.

## Fixtures

A developer should be able to run Python scripts containing UV script metadata, which generate toy Parquet files and convert them to AnnData-Zarr Dataframe format. The fixture generation script should depend on: numpy, pyarrow, anndata, and zarr python packages for the zarr v3 format. After running the conversion scripts, the user should obtain structurally equivalent Parquet and AnnData-Zarr dataframes on-disk in the generated fixture files.

Note: The Parquet and Zarr fixtures are generated independently from the same source dataframe, so they may use different compression and chunking. The store implementation bridges this gap.

## Tech stack:

- Python scripts using argparse and UV inline script metadata for dependencies: numpy, pyarrow, anndata, and zarr.
- PNPM for package management and running scripts
- Typescript
- Zarrita.js
- Hyparquet: either as a dependency or its internals vendored
- Vitest for unit tests
