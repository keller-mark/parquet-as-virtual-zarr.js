# arrow-as-zarr

Read and write Apache Arrow data using Zarr.

## Installation

```bash
pip install arrow-as-zarr
```

Or install from source:

```bash
git clone https://github.com/keller-mark/arrow-as-zarr.git
cd arrow-as-zarr
pip install -e .
```

## Usage

### Writing Arrow data to Zarr

```python
import pyarrow as pa
from arrow_as_zarr import write_arrow_to_zarr

# Create an Arrow table
table = pa.table({
    "x": [1, 2, 3, 4, 5],
    "y": [1.0, 2.0, 3.0, 4.0, 5.0],
    "z": [10, 20, 30, 40, 50],
})

# Write to Zarr format
root = write_arrow_to_zarr(table, "data.zarr")

# Access data through the returned Zarr group
print(root["x"][:])  # [1 2 3 4 5]
```

### Reading Zarr data as Arrow

```python
from arrow_as_zarr import read_arrow_from_zarr

# Read Zarr store as Arrow table
table = read_arrow_from_zarr("data.zarr")

# Convert to pandas
df = table.to_pandas()
print(df)

# Read only specific columns
table = read_arrow_from_zarr("data.zarr", columns=["x", "y"])
```

### Using ArrowStore directly with Zarr

```python
import pyarrow as pa
import zarr
from arrow_as_zarr import ArrowStore

# Create an Arrow table
table = pa.table({
    "x": [1, 2, 3],
    "y": [4.0, 5.0, 6.0],
})

# Create a Zarr-compatible store backed by Arrow data
store = ArrowStore(table)

# Open with Zarr
root = zarr.open_group(store, mode="r")

# Access arrays
print(root["x"][:])  # [1 2 3]
print(root["y"][:])  # [4. 5. 6.]
```

### Loading from files

```python
from arrow_as_zarr import ArrowStore

# Load from Arrow IPC file
store = ArrowStore.from_file("data.arrow")

# Load from Parquet file
store = ArrowStore.from_parquet("data.parquet")
```

## Supported Data Types

The following Arrow data types are supported:

- Integers: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- Floats: `float16`, `float32`, `float64`
- Boolean: `bool`
- Strings: `string`, `large_string` (converted to object arrays)

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=arrow_as_zarr
```

## License

MIT License