#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "numpy",
#   "pandas",
#   "pyarrow",
#   "anndata>=0.12",
#   "zarr>=3.0",
# ]
# ///

"""Generate test fixtures: structurally equivalent Parquet and AnnData-Zarr dataframe files."""

import argparse
import os
import shutil

import anndata as ad
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import zarr

# Pandas 3.x defaults to Arrow-backed strings, which anndata's zarr writer does not support.
# Force numpy-backed object strings throughout.
pd.options.future.infer_string = False


def make_obs(n_obs: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "cell_type": pd.Categorical(
                np.array(rng.choice(["T cell", "B cell", "NK cell"], n_obs), dtype=object),
                categories=np.array(["B cell", "NK cell", "T cell"], dtype=object),
            ),
            "leiden": pd.Categorical(
                np.array(rng.choice(["0", "1", "2", "3"], n_obs), dtype=object),
                categories=np.array(["0", "1", "2", "3"], dtype=object),
            ),
            "n_counts": rng.uniform(500.0, 5000.0, n_obs).astype(np.float32),
            "n_genes": rng.integers(200, 2000, n_obs).astype(np.int32),
        },
        index=pd.Index([f"cell_{i}" for i in range(n_obs)], name="obs_id", dtype=object),
    )


def write_parquet(obs: pd.DataFrame, path: str, row_group_size: int) -> None:
    table = pa.Table.from_pandas(obs)
    pq.write_table(table, path, row_group_size=row_group_size)
    meta = pq.read_metadata(path)
    print(f"Wrote Parquet: {path}")
    print(f"  rows={meta.num_rows}  row_groups={meta.num_row_groups}  columns={meta.num_columns}")


def make_obs_plain(n_obs: int, seed: int = 42) -> pd.DataFrame:
    """Create a dataframe with only non-nullable numeric and string columns.

    Unlike make_obs(), this produces columns that will be written with PLAIN
    encoding and REQUIRED repetition (no dictionary, no nulls), enabling
    zero-copy pass-through in the parquet store.
    """
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "n_counts": rng.uniform(500.0, 5000.0, n_obs).astype(np.float32),
            "n_genes": rng.integers(200, 2000, n_obs).astype(np.int32),
        },
        index=pd.Index([f"cell_{i}" for i in range(n_obs)], name="obs_id", dtype=object),
    )


def write_parquet_plain(obs: pd.DataFrame, path: str, row_group_size: int, compression: str = "SNAPPY") -> None:
    """Write a parquet file with PLAIN encoding (no dictionary) and explicit
    non-nullable Arrow schema for numeric columns."""
    # Build an Arrow schema where numeric fields are non-nullable (required)
    arrow_fields = []
    pandas_meta = pa.Table.from_pandas(obs).schema.pandas_metadata
    for col in obs.columns:
        if obs[col].dtype == np.float32:
            arrow_fields.append(pa.field(col, pa.float32(), nullable=False))
        elif obs[col].dtype == np.int32:
            arrow_fields.append(pa.field(col, pa.int32(), nullable=False))
        else:
            arrow_fields.append(pa.field(col, pa.string()))
    # Include the index as a non-nullable string column
    arrow_fields.insert(0, pa.field(obs.index.name, pa.string()))
    import json
    pandas_meta_bytes = json.dumps(pa.Table.from_pandas(obs).schema.pandas_metadata).encode("utf-8")
    schema = pa.schema(arrow_fields, metadata={b"pandas": pandas_meta_bytes})

    # Build arrays manually so we control nullability
    arrays = [pa.array(obs.index.to_list(), type=pa.string())]
    for col in obs.columns:
        if obs[col].dtype == np.float32:
            arrays.append(pa.array(obs[col].values, type=pa.float32(), mask=None))
        elif obs[col].dtype == np.int32:
            arrays.append(pa.array(obs[col].values, type=pa.int32(), mask=None))
        else:
            arrays.append(pa.array(obs[col].to_list(), type=pa.string()))
    table = pa.table(dict(zip([f.name for f in schema], arrays)), schema=schema)
    pq.write_table(
        table,
        path,
        row_group_size=row_group_size,
        use_dictionary=False,
        compression=compression,
    )
    meta = pq.read_metadata(path)
    print(f"Wrote Parquet (plain, {compression}): {path}")
    print(f"  rows={meta.num_rows}  row_groups={meta.num_row_groups}  columns={meta.num_columns}")


def write_parquet_multipart(obs: pd.DataFrame, path: str, row_group_size: int) -> None:
    """Write a multi-part parquet directory with one part file per row group."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    table = pa.Table.from_pandas(obs)
    part_index = 0
    for i in range(0, len(obs), row_group_size):
        part_table = table.slice(i, min(row_group_size, len(obs) - i))
        part_path = os.path.join(path, f"part.{part_index}.parquet")
        pq.write_table(part_table, part_path, row_group_size=row_group_size)
        part_index += 1
    print(f"Wrote multi-part Parquet: {path}")
    print(f"  parts={part_index}  rows_per_part={row_group_size}")


def write_zarr(obs: pd.DataFrame, path: str, row_group_size: int) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
    ad.settings.zarr_write_format = 3
    adata = ad.AnnData(obs=obs)
    adata.write_zarr(path, chunks=row_group_size)
    zarr.consolidate_metadata(path + "/obs")
    print(f"Wrote AnnData-Zarr (v3): {path}")
    print(f"  obs dataframe at: {path}/obs/")
    print(f"  columns: {list(obs.columns)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-obs", type=int, default=100, help="Number of observations")
    parser.add_argument("--row-group-size", type=int, default=25, help="Row group size")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "output"),
        help="Output directory",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    obs = make_obs(args.n_obs, seed=args.seed)

    parquet_path = os.path.join(args.output_dir, "obs.parquet")
    write_parquet(obs, parquet_path, args.row_group_size)

    parquet_multipart_path = os.path.join(args.output_dir, "obs_multipart")
    write_parquet_multipart(obs, parquet_multipart_path, args.row_group_size)

    zarr_path = os.path.join(args.output_dir, "adata.zarr")
    write_zarr(obs, zarr_path, args.row_group_size)

    # Generate PLAIN-encoded fixtures for zero-copy testing
    obs_plain = make_obs_plain(args.n_obs, seed=args.seed)

    parquet_plain_snappy_path = os.path.join(args.output_dir, "obs_plain_snappy.parquet")
    write_parquet_plain(obs_plain, parquet_plain_snappy_path, args.row_group_size, compression="SNAPPY")

    parquet_plain_none_path = os.path.join(args.output_dir, "obs_plain_none.parquet")
    write_parquet_plain(obs_plain, parquet_plain_none_path, args.row_group_size, compression="NONE")

    zarr_plain_path = os.path.join(args.output_dir, "adata_plain.zarr")
    write_zarr(obs_plain, zarr_plain_path, args.row_group_size)


if __name__ == "__main__":
    main()
