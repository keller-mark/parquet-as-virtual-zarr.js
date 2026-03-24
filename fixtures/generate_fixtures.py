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


def write_zarr(obs: pd.DataFrame, path: str, row_group_size: int) -> None:
    if os.path.exists(path):
        shutil.rmtree(path)
    adata = ad.AnnData(obs=obs)
    adata.write_zarr(path, chunks=[row_group_size])
    print(f"Wrote AnnData-Zarr: {path}")
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

    zarr_path = os.path.join(args.output_dir, "adata.zarr")
    write_zarr(obs, zarr_path, args.row_group_size)


if __name__ == "__main__":
    main()
