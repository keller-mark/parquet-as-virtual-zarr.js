"""Tests for I/O functions."""

import pytest
import pyarrow as pa
import zarr
import numpy as np
import tempfile
import os

from arrow_as_zarr import write_arrow_to_zarr, read_arrow_from_zarr


class TestWriteArrowToZarr:
    """Test write_arrow_to_zarr function."""
    
    def test_write_basic_table(self, tmp_path):
        """Test writing a basic Arrow table to Zarr."""
        table = pa.table({
            "x": [1, 2, 3],
            "y": [4.0, 5.0, 6.0],
        })
        
        zarr_path = str(tmp_path / "test.zarr")
        root = write_arrow_to_zarr(table, zarr_path)
        
        assert "x" in root
        assert "y" in root
        np.testing.assert_array_equal(root["x"][:], [1, 2, 3])
        np.testing.assert_array_almost_equal(root["y"][:], [4.0, 5.0, 6.0])
    
    def test_write_record_batch(self, tmp_path):
        """Test writing a RecordBatch to Zarr."""
        batch = pa.RecordBatch.from_pydict({
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
        })
        
        zarr_path = str(tmp_path / "test.zarr")
        root = write_arrow_to_zarr(batch, zarr_path)
        
        assert "a" in root
        assert "b" in root
    
    def test_write_with_chunks(self, tmp_path):
        """Test writing with custom chunk size."""
        table = pa.table({"x": list(range(100))})
        
        zarr_path = str(tmp_path / "test.zarr")
        root = write_arrow_to_zarr(table, zarr_path, chunks=10)
        
        assert root["x"].chunks == (10,)
    
    def test_write_preserves_metadata(self, tmp_path):
        """Test that schema metadata is preserved."""
        table = pa.table({
            "x": [1, 2, 3],
            "y": [4.0, 5.0, 6.0],
        })
        
        zarr_path = str(tmp_path / "test.zarr")
        root = write_arrow_to_zarr(table, zarr_path)
        
        assert "column_names" in root.attrs
        assert root.attrs["column_names"] == ["x", "y"]
        assert root.attrs["num_rows"] == 3
        assert root.attrs["num_columns"] == 2
    
    def test_write_invalid_input_raises_type_error(self, tmp_path):
        """Test that invalid input raises TypeError."""
        zarr_path = str(tmp_path / "test.zarr")
        
        with pytest.raises(TypeError):
            write_arrow_to_zarr([1, 2, 3], zarr_path)


class TestReadArrowFromZarr:
    """Test read_arrow_from_zarr function."""
    
    def test_read_basic_zarr(self, tmp_path):
        """Test reading a basic Zarr store as Arrow."""
        # First write
        table = pa.table({
            "x": [1, 2, 3],
            "y": [4.0, 5.0, 6.0],
        })
        
        zarr_path = str(tmp_path / "test.zarr")
        write_arrow_to_zarr(table, zarr_path)
        
        # Then read
        result = read_arrow_from_zarr(zarr_path)
        
        assert "x" in result.column_names
        assert "y" in result.column_names
        np.testing.assert_array_equal(result["x"].to_numpy(), [1, 2, 3])
        np.testing.assert_array_almost_equal(result["y"].to_numpy(), [4.0, 5.0, 6.0])
    
    def test_read_selected_columns(self, tmp_path):
        """Test reading only selected columns."""
        table = pa.table({
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "c": [7, 8, 9],
        })
        
        zarr_path = str(tmp_path / "test.zarr")
        write_arrow_to_zarr(table, zarr_path)
        
        result = read_arrow_from_zarr(zarr_path, columns=["a", "c"])
        
        assert "a" in result.column_names
        assert "c" in result.column_names
        assert "b" not in result.column_names
    
    def test_roundtrip(self, tmp_path):
        """Test write then read preserves data."""
        original = pa.table({
            "int_col": pa.array([1, 2, 3], type=pa.int64()),
            "float_col": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
        })
        
        zarr_path = str(tmp_path / "test.zarr")
        write_arrow_to_zarr(original, zarr_path)
        result = read_arrow_from_zarr(zarr_path)
        
        np.testing.assert_array_equal(
            original["int_col"].to_numpy(),
            result["int_col"].to_numpy()
        )
        np.testing.assert_array_almost_equal(
            original["float_col"].to_numpy(),
            result["float_col"].to_numpy()
        )


class TestDataTypeRoundtrip:
    """Test roundtrip for various data types."""
    
    @pytest.mark.parametrize("arrow_type,values", [
        (pa.int8(), [-128, 0, 127]),
        (pa.int16(), [-32768, 0, 32767]),
        (pa.int32(), [-2147483648, 0, 2147483647]),
        (pa.int64(), [-9223372036854775808, 0, 9223372036854775807]),
        (pa.uint8(), [0, 128, 255]),
        (pa.uint16(), [0, 32768, 65535]),
        (pa.uint32(), [0, 2147483648, 4294967295]),
        (pa.float32(), [1.5, 2.5, 3.5]),
        (pa.float64(), [1.123456789, 2.987654321, 3.141592653]),
    ])
    def test_numeric_types_roundtrip(self, tmp_path, arrow_type, values):
        """Test roundtrip for numeric types."""
        original = pa.table({"x": pa.array(values, type=arrow_type)})
        
        zarr_path = str(tmp_path / "test.zarr")
        write_arrow_to_zarr(original, zarr_path)
        result = read_arrow_from_zarr(zarr_path)
        
        if pa.types.is_floating(arrow_type):
            np.testing.assert_array_almost_equal(
                result["x"].to_numpy(),
                values,
                decimal=5 if arrow_type == pa.float32() else 9
            )
        else:
            np.testing.assert_array_equal(result["x"].to_numpy(), values)


class TestEdgeCases:
    """Test edge cases for I/O functions."""
    
    def test_empty_table_roundtrip(self, tmp_path):
        """Test roundtrip with empty table."""
        original = pa.table({"x": pa.array([], type=pa.int64())})
        
        zarr_path = str(tmp_path / "test.zarr")
        write_arrow_to_zarr(original, zarr_path)
        result = read_arrow_from_zarr(zarr_path)
        
        assert result.num_rows == 0
    
    def test_single_row_roundtrip(self, tmp_path):
        """Test roundtrip with single row."""
        original = pa.table({"x": [42]})
        
        zarr_path = str(tmp_path / "test.zarr")
        write_arrow_to_zarr(original, zarr_path)
        result = read_arrow_from_zarr(zarr_path)
        
        assert result.num_rows == 1
        assert result["x"][0].as_py() == 42
    
    def test_large_table_roundtrip(self, tmp_path):
        """Test roundtrip with larger table."""
        n = 10000
        original = pa.table({
            "x": list(range(n)),
            "y": [float(i) for i in range(n)],
        })
        
        zarr_path = str(tmp_path / "test.zarr")
        write_arrow_to_zarr(original, zarr_path, chunks=1000)
        result = read_arrow_from_zarr(zarr_path)
        
        assert result.num_rows == n
        np.testing.assert_array_equal(
            result["x"].to_numpy(),
            original["x"].to_numpy()
        )
