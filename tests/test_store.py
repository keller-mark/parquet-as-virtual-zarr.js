"""Tests for ArrowStore class."""

import pytest
import pyarrow as pa
import zarr
import numpy as np

from arrow_as_zarr import ArrowStore


class TestArrowStoreBasic:
    """Test basic ArrowStore functionality."""
    
    def test_create_from_table(self):
        """Test creating an ArrowStore from a pyarrow Table."""
        table = pa.table({
            "int_col": [1, 2, 3],
            "float_col": [1.0, 2.0, 3.0],
        })
        store = ArrowStore(table)
        assert store.table.equals(table)
    
    def test_create_from_record_batch(self):
        """Test creating an ArrowStore from a RecordBatch."""
        batch = pa.RecordBatch.from_pydict({
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
        })
        store = ArrowStore(batch)
        assert store.table.num_rows == 3
    
    def test_invalid_input_raises_type_error(self):
        """Test that invalid input raises TypeError."""
        with pytest.raises(TypeError):
            ArrowStore([1, 2, 3])
        
        with pytest.raises(TypeError):
            ArrowStore({"a": [1, 2, 3]})


class TestArrowStoreMapping:
    """Test MutableMapping interface of ArrowStore."""
    
    def test_keys(self):
        """Test that keys returns expected store keys."""
        table = pa.table({"x": [1, 2, 3]})
        store = ArrowStore(table)
        
        keys = list(store.keys())
        assert ".zgroup" in keys
        assert "x/.zarray" in keys
        assert "x/0" in keys
    
    def test_contains(self):
        """Test __contains__ method."""
        table = pa.table({"x": [1, 2, 3]})
        store = ArrowStore(table)
        
        assert ".zgroup" in store
        assert "x/.zarray" in store
        assert "x/0" in store
        assert "nonexistent" not in store
    
    def test_getitem(self):
        """Test __getitem__ method."""
        table = pa.table({"x": [1, 2, 3]})
        store = ArrowStore(table)
        
        # Should return bytes
        assert isinstance(store[".zgroup"], bytes)
        assert isinstance(store["x/.zarray"], bytes)
    
    def test_getitem_missing_raises_keyerror(self):
        """Test that accessing missing key raises KeyError."""
        table = pa.table({"x": [1, 2, 3]})
        store = ArrowStore(table)
        
        with pytest.raises(KeyError):
            _ = store["nonexistent"]
    
    def test_len(self):
        """Test __len__ method."""
        table = pa.table({"x": [1, 2, 3]})
        store = ArrowStore(table)
        
        # Should have .zgroup, x/.zarray, and x/0
        assert len(store) == 3
    
    def test_iter(self):
        """Test __iter__ method."""
        table = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
        store = ArrowStore(table)
        
        keys = list(store)
        assert ".zgroup" in keys
        assert "x/.zarray" in keys
        assert "y/.zarray" in keys


class TestArrowStoreWithZarr:
    """Test using ArrowStore with Zarr."""
    
    def test_open_as_zarr_group(self):
        """Test opening ArrowStore as a Zarr group."""
        table = pa.table({
            "x": [1, 2, 3],
            "y": [4.0, 5.0, 6.0],
        })
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        assert "x" in root
        assert "y" in root
    
    def test_read_int_array(self):
        """Test reading integer array through Zarr."""
        table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        np.testing.assert_array_equal(arr, [1, 2, 3])
    
    def test_read_float_array(self):
        """Test reading float array through Zarr."""
        table = pa.table({"x": pa.array([1.5, 2.5, 3.5], type=pa.float64())})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        np.testing.assert_array_almost_equal(arr, [1.5, 2.5, 3.5])
    
    def test_multiple_columns(self):
        """Test reading multiple columns through Zarr."""
        table = pa.table({
            "a": [1, 2, 3],
            "b": [10.0, 20.0, 30.0],
            "c": [100, 200, 300],
        })
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        
        np.testing.assert_array_equal(root["a"][:], [1, 2, 3])
        np.testing.assert_array_almost_equal(root["b"][:], [10.0, 20.0, 30.0])
        np.testing.assert_array_equal(root["c"][:], [100, 200, 300])


class TestArrowStoreDataTypes:
    """Test various Arrow data types with ArrowStore."""
    
    @pytest.mark.parametrize("arrow_type,expected_values", [
        (pa.int8(), [-128, 0, 127]),
        (pa.int16(), [-32768, 0, 32767]),
        (pa.int32(), [-2147483648, 0, 2147483647]),
        (pa.int64(), [-9223372036854775808, 0, 9223372036854775807]),
        (pa.uint8(), [0, 128, 255]),
        (pa.uint16(), [0, 32768, 65535]),
        (pa.uint32(), [0, 2147483648, 4294967295]),
        (pa.uint64(), [0, 9223372036854775808, 18446744073709551615]),
    ])
    def test_integer_types(self, arrow_type, expected_values):
        """Test various integer types."""
        table = pa.table({"x": pa.array(expected_values, type=arrow_type)})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        np.testing.assert_array_equal(arr, expected_values)
    
    def test_float32(self):
        """Test float32 type."""
        values = [1.5, 2.5, 3.5]
        table = pa.table({"x": pa.array(values, type=pa.float32())})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        np.testing.assert_array_almost_equal(arr, values, decimal=5)
    
    def test_float64(self):
        """Test float64 type."""
        values = [1.123456789, 2.987654321, 3.141592653]
        table = pa.table({"x": pa.array(values, type=pa.float64())})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        np.testing.assert_array_almost_equal(arr, values)


class TestArrowStoreEdgeCases:
    """Test edge cases for ArrowStore."""
    
    def test_empty_table(self):
        """Test with an empty table."""
        table = pa.table({"x": pa.array([], type=pa.int64())})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        assert len(arr) == 0
    
    def test_single_row(self):
        """Test with a single row."""
        table = pa.table({"x": [42]})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        assert len(arr) == 1
        assert arr[0] == 42
    
    def test_large_array(self):
        """Test with a larger array."""
        n = 10000
        values = list(range(n))
        table = pa.table({"x": values})
        store = ArrowStore(table)
        
        root = zarr.open_group(store, mode="r")
        arr = root["x"][:]
        
        assert len(arr) == n
        np.testing.assert_array_equal(arr, values)
