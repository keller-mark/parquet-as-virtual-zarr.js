"""I/O functions for reading and writing Arrow data using Zarr."""

from typing import Optional, Union
import json

import numpy as np
import pyarrow as pa
import zarr


def write_arrow_to_zarr(
    table: Union[pa.Table, pa.RecordBatch],
    zarr_path: str,
    chunks: Optional[int] = None,
) -> zarr.Group:
    """
    Write an Apache Arrow Table or RecordBatch to a Zarr store on disk.
    
    Each column in the Arrow table becomes a Zarr array in the store.
    The Arrow schema metadata is preserved in the Zarr group attributes.
    
    Parameters
    ----------
    table : pyarrow.Table or pyarrow.RecordBatch
        The Arrow data to write.
    zarr_path : str
        Path to write the Zarr store.
    chunks : int, optional
        Chunk size for the Zarr arrays. If not specified, uses the full
        length of the arrays (single chunk).
    
    Returns
    -------
    zarr.Group
        The root Zarr group.
    
    Examples
    --------
    >>> import pyarrow as pa
    >>> from arrow_as_zarr import write_arrow_to_zarr
    >>> 
    >>> table = pa.table({
    ...     "x": [1, 2, 3],
    ...     "y": [4.0, 5.0, 6.0]
    ... })
    >>> root = write_arrow_to_zarr(table, "data.zarr")
    """
    if isinstance(table, pa.RecordBatch):
        table = pa.Table.from_batches([table])
    
    if not isinstance(table, pa.Table):
        raise TypeError(
            f"Expected pyarrow.Table or pyarrow.RecordBatch, got {type(table)}"
        )
    
    # Create the root group
    root = zarr.open_group(zarr_path, mode="w")
    
    # Store Arrow schema metadata as group attributes
    schema = table.schema
    schema_metadata = {
        "arrow_schema": schema.to_string(),
        "num_columns": len(schema),
        "num_rows": table.num_rows,
        "column_names": [field.name for field in schema],
        "column_types": [str(field.type) for field in schema],
    }
    root.attrs.update(schema_metadata)
    
    # Write each column as a Zarr array
    num_rows = table.num_rows
    # Use chunk_size of 1 minimum to avoid division by zero with empty arrays
    chunk_size = chunks if chunks is not None else max(num_rows, 1)
    
    for i, field in enumerate(schema):
        col_name = field.name
        col = table.column(i)
        arrow_type = field.type
        
        # Convert Arrow column to numpy array
        try:
            if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
                # Handle string columns
                arr = col.to_pandas().values
                dtype = object
            else:
                arr = col.to_numpy()
                dtype = arr.dtype
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            # Skip columns that can't be converted
            continue
        
        # Create Zarr array
        root.create_dataset(
            col_name,
            data=arr,
            chunks=(chunk_size,),
            dtype=dtype,
        )
    
    return root


def read_arrow_from_zarr(
    zarr_path: str,
    columns: Optional[list[str]] = None,
) -> pa.Table:
    """
    Read a Zarr store into an Apache Arrow Table.
    
    Parameters
    ----------
    zarr_path : str
        Path to the Zarr store.
    columns : list[str], optional
        List of column names to read. If not specified, reads all columns.
    
    Returns
    -------
    pyarrow.Table
        The Arrow table containing the data.
    
    Examples
    --------
    >>> from arrow_as_zarr import read_arrow_from_zarr
    >>> 
    >>> table = read_arrow_from_zarr("data.zarr")
    >>> print(table.to_pandas())
    """
    # Open the Zarr group
    root = zarr.open_group(zarr_path, mode="r")
    
    # Get column names from attributes or array names
    if "column_names" in root.attrs:
        available_columns = root.attrs["column_names"]
    else:
        available_columns = list(root.array_keys())
    
    # Filter columns if specified
    if columns is not None:
        selected_columns = [c for c in columns if c in available_columns]
    else:
        selected_columns = available_columns
    
    # Read each column and convert to Arrow
    arrays = {}
    for col_name in selected_columns:
        if col_name not in root:
            continue
        arr = root[col_name][:]
        
        # Convert numpy array to Arrow array
        if arr.dtype == object:
            # String array
            arrays[col_name] = pa.array(arr, type=pa.string())
        else:
            arrays[col_name] = pa.array(arr)
    
    return pa.table(arrays)
