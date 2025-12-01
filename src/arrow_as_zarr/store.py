"""ArrowStore - A Zarr store backed by Apache Arrow data."""

from collections.abc import MutableMapping
from typing import Iterator, Optional, Union
import json

import numpy as np
import pyarrow as pa


def _get_zarr_dtype(arrow_type: pa.DataType) -> str:
    """Convert Arrow type to Zarr dtype string."""
    if pa.types.is_int8(arrow_type):
        return "<i1"
    elif pa.types.is_int16(arrow_type):
        return "<i2"
    elif pa.types.is_int32(arrow_type):
        return "<i4"
    elif pa.types.is_int64(arrow_type):
        return "<i8"
    elif pa.types.is_uint8(arrow_type):
        return "|u1"
    elif pa.types.is_uint16(arrow_type):
        return "<u2"
    elif pa.types.is_uint32(arrow_type):
        return "<u4"
    elif pa.types.is_uint64(arrow_type):
        return "<u8"
    elif pa.types.is_float16(arrow_type):
        return "<f2"
    elif pa.types.is_float32(arrow_type):
        return "<f4"
    elif pa.types.is_float64(arrow_type):
        return "<f8"
    elif pa.types.is_boolean(arrow_type):
        return "|b1"
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        # For strings, we'll use object dtype
        return "|O"
    else:
        raise ValueError(f"Unsupported Arrow type: {arrow_type}")


def _create_zarray_metadata(
    shape: tuple, dtype: str, chunks: Optional[tuple] = None
) -> dict:
    """Create .zarray metadata for a Zarr array."""
    if chunks is None:
        # Use minimum chunk size of 1 to avoid division by zero with empty arrays
        chunks = tuple(max(s, 1) for s in shape)
    
    return {
        "zarr_format": 2,
        "shape": list(shape),
        "chunks": list(chunks),
        "dtype": dtype,
        "compressor": None,
        "fill_value": None if dtype == "|O" else 0,
        "order": "C",
        "filters": None,
    }


def _create_zgroup_metadata() -> dict:
    """Create .zgroup metadata for a Zarr group."""
    return {"zarr_format": 2}


class ArrowStore(MutableMapping):
    """
    A Zarr store backed by an Apache Arrow Table or RecordBatch.
    
    This class implements the MutableMapping interface required by Zarr,
    allowing Arrow data to be accessed as if it were a Zarr store.
    
    Each column in the Arrow table becomes a Zarr array in the store.
    
    Parameters
    ----------
    table : pyarrow.Table or pyarrow.RecordBatch
        The Arrow data to expose as a Zarr store.
    
    Examples
    --------
    >>> import pyarrow as pa
    >>> import zarr
    >>> from arrow_as_zarr import ArrowStore
    >>> 
    >>> # Create Arrow data
    >>> table = pa.table({
    ...     "x": [1, 2, 3],
    ...     "y": [4.0, 5.0, 6.0]
    ... })
    >>> 
    >>> # Create store and open with Zarr
    >>> store = ArrowStore(table)
    >>> root = zarr.open_group(store, mode="r")
    >>> print(root["x"][:])
    [1 2 3]
    """
    
    def __init__(self, table: Union[pa.Table, pa.RecordBatch]) -> None:
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])
        
        if not isinstance(table, pa.Table):
            raise TypeError(
                f"Expected pyarrow.Table or pyarrow.RecordBatch, got {type(table)}"
            )
        
        self._table = table
        self._cache: dict[str, bytes] = {}
        self._build_cache()
    
    def _build_cache(self) -> None:
        """Build the internal cache with Zarr metadata and data chunks."""
        # Add group metadata
        self._cache[".zgroup"] = json.dumps(_create_zgroup_metadata()).encode("utf-8")
        
        # Add metadata and data for each column
        schema = self._table.schema
        num_rows = self._table.num_rows
        
        for i, field in enumerate(schema):
            col_name = field.name
            col = self._table.column(i)
            arrow_type = field.type
            
            try:
                zarr_dtype = _get_zarr_dtype(arrow_type)
            except ValueError:
                # Skip unsupported types
                continue
            
            # Create .zarray metadata
            shape = (num_rows,)
            zarray_meta = _create_zarray_metadata(shape, zarr_dtype)
            self._cache[f"{col_name}/.zarray"] = json.dumps(zarray_meta).encode("utf-8")
            
            # Convert data to bytes
            if zarr_dtype == "|O":
                # For string columns, we need special handling
                # Convert to numpy object array and use pickle
                import pickle
                arr = col.to_pandas().values
                self._cache[f"{col_name}/0"] = pickle.dumps(arr)
            else:
                # For numeric types, convert to numpy and get bytes
                arr = col.to_numpy()
                self._cache[f"{col_name}/0"] = arr.tobytes()
    
    @property
    def table(self) -> pa.Table:
        """Return the underlying Arrow table."""
        return self._table
    
    def __getitem__(self, key: str) -> bytes:
        """Get an item from the store."""
        if key not in self._cache:
            raise KeyError(key)
        return self._cache[key]
    
    def __setitem__(self, key: str, value: bytes) -> None:
        """Set an item in the store (limited support for read-only stores)."""
        self._cache[key] = value
    
    def __delitem__(self, key: str) -> None:
        """Delete an item from the store."""
        if key not in self._cache:
            raise KeyError(key)
        del self._cache[key]
    
    def __iter__(self) -> Iterator[str]:
        """Iterate over keys in the store."""
        return iter(self._cache)
    
    def __len__(self) -> int:
        """Return the number of items in the store."""
        return len(self._cache)
    
    def __contains__(self, key: object) -> bool:
        """Check if a key is in the store."""
        return key in self._cache
    
    def keys(self):
        """Return the keys in the store."""
        return self._cache.keys()
    
    def values(self):
        """Return the values in the store."""
        return self._cache.values()
    
    def items(self):
        """Return the items in the store."""
        return self._cache.items()
    
    @classmethod
    def from_file(cls, path: str) -> "ArrowStore":
        """
        Create an ArrowStore from an Arrow IPC file.
        
        Parameters
        ----------
        path : str
            Path to the Arrow IPC file.
        
        Returns
        -------
        ArrowStore
            A new ArrowStore backed by the Arrow data.
        """
        table = pa.ipc.open_file(path).read_all()
        return cls(table)
    
    @classmethod
    def from_parquet(cls, path: str) -> "ArrowStore":
        """
        Create an ArrowStore from a Parquet file.
        
        Parameters
        ----------
        path : str
            Path to the Parquet file.
        
        Returns
        -------
        ArrowStore
            A new ArrowStore backed by the Arrow data.
        """
        import pyarrow.parquet as pq
        table = pq.read_table(path)
        return cls(table)
