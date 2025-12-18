import io
import os
import tempfile

import pandas as pd
import pytest

from metrics_utility.library.storage import load_csv, load_json, load_parquet, save_csv, save_json, save_parquet


# Test data
test_dict_list = [
    {'name': 'Alice', 'age': 30, 'city': 'NYC'},
    {'name': 'Bob', 'age': 25, 'city': 'LA'},
]

test_dict_list_with_unicode = [
    {'name': 'Müller', 'age': 30, 'city': 'München'},
    {'name': 'José', 'age': 25, 'city': 'São Paulo'},
]

test_json_data = {'users': test_dict_list, 'count': 2}
test_json_data_with_unicode = {'message': 'Hello 世界 🌍', 'emoji': '🎉'}


def test_csv_save_load_list_of_dicts_filename():
    """Test save_csv and load_csv with list of dicts using filename."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        csv_path = f.name

    try:
        save_csv(test_dict_list, filename=csv_path)
        loaded = load_csv(csv_path)
        assert loaded == test_dict_list
    finally:
        os.unlink(csv_path)


def test_csv_save_load_dataframe_filename():
    """Test save_csv and load_csv with DataFrame using filename."""
    test_df = pd.DataFrame(test_dict_list)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        csv_path = f.name

    try:
        save_csv(test_df, filename=csv_path)
        loaded = load_csv(csv_path)
        assert loaded == test_dict_list
    finally:
        os.unlink(csv_path)


def test_csv_save_load_fileobj():
    """Test save_csv and load_csv with file objects."""
    csv_buffer = io.StringIO()
    save_csv(test_dict_list, fileobj=csv_buffer)

    csv_buffer.seek(0)
    loaded = load_csv(csv_buffer)
    assert loaded == test_dict_list


def test_csv_utf8_encoding():
    """Test that CSV files handle UTF-8 characters correctly."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        csv_path = f.name

    try:
        save_csv(test_dict_list_with_unicode, filename=csv_path)
        loaded = load_csv(csv_path)
        assert loaded == test_dict_list_with_unicode
        assert loaded[0]['name'] == 'Müller'
        assert loaded[0]['city'] == 'München'
    finally:
        os.unlink(csv_path)


def test_csv_empty_list():
    """Test save_csv with an empty list."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        csv_path = f.name

    try:
        save_csv([], filename=csv_path)
        # File should exist but be empty
        assert os.path.exists(csv_path)
        assert os.path.getsize(csv_path) == 0
    finally:
        os.unlink(csv_path)


def test_json_save_load_filename():
    """Test save_json and load_json using filename."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json_path = f.name

    try:
        save_json(test_json_data, filename=json_path)
        loaded = load_json(json_path)
        assert loaded == test_json_data
    finally:
        os.unlink(json_path)


def test_json_save_load_fileobj():
    """Test save_json and load_json with file objects."""
    json_buffer = io.StringIO()
    save_json(test_json_data, fileobj=json_buffer)

    json_buffer.seek(0)
    loaded = load_json(json_buffer)
    assert loaded == test_json_data


def test_json_utf8_encoding():
    """Test that JSON files handle UTF-8 characters correctly."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json_path = f.name

    try:
        save_json(test_json_data_with_unicode, filename=json_path)
        loaded = load_json(json_path)
        assert loaded == test_json_data_with_unicode
        assert loaded['message'] == 'Hello 世界 🌍'
        assert loaded['emoji'] == '🎉'
    finally:
        os.unlink(json_path)


def test_parquet_save_load_filename():
    """Test save_parquet and load_parquet using filename."""
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        pytest.skip('pyarrow not available')

    test_df = pd.DataFrame(test_dict_list)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.parquet', delete=False) as f:
        parquet_path = f.name

    try:
        save_parquet(test_df, filename=parquet_path)
        loaded = load_parquet(parquet_path)
        assert test_df.equals(loaded)
    finally:
        os.unlink(parquet_path)


def test_parquet_save_load_fileobj():
    """Test save_parquet and load_parquet with file objects."""
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        pytest.skip('pyarrow not available')

    test_df = pd.DataFrame(test_dict_list)
    parquet_buffer = io.BytesIO()

    save_parquet(test_df, fileobj=parquet_buffer)
    parquet_buffer.seek(0)
    loaded = load_parquet(parquet_buffer)
    assert test_df.equals(loaded)


def test_save_csv_error_no_args():
    """Test that save_csv raises ValueError when neither filename nor fileobj is provided."""
    with pytest.raises(ValueError, match='Exactly one of filename or fileobj must be provided'):
        save_csv(test_dict_list)


def test_save_csv_error_both_args():
    """Test that save_csv raises ValueError when both filename and fileobj are provided."""
    with pytest.raises(ValueError, match='Exactly one of filename or fileobj must be provided'):
        save_csv(test_dict_list, filename='test.csv', fileobj=io.StringIO())


def test_save_csv_error_invalid_type():
    """Test that save_csv raises TypeError for invalid data type."""
    with pytest.raises(TypeError, match='data must be a DataFrame or list of dicts'):
        save_csv('invalid data', filename='test.csv')


def test_save_json_error_no_args():
    """Test that save_json raises ValueError when neither filename nor fileobj is provided."""
    with pytest.raises(ValueError, match='Exactly one of filename or fileobj must be provided'):
        save_json(test_json_data)


def test_save_json_error_both_args():
    """Test that save_json raises ValueError when both filename and fileobj are provided."""
    with pytest.raises(ValueError, match='Exactly one of filename or fileobj must be provided'):
        save_json(test_json_data, filename='test.json', fileobj=io.StringIO())


def test_save_parquet_error_no_args():
    """Test that save_parquet raises ValueError when neither filename nor fileobj is provided."""
    test_df = pd.DataFrame(test_dict_list)
    with pytest.raises(ValueError, match='Exactly one of filename or fileobj must be provided'):
        save_parquet(test_df)


def test_save_parquet_error_both_args():
    """Test that save_parquet raises ValueError when both filename and fileobj are provided."""
    test_df = pd.DataFrame(test_dict_list)
    with pytest.raises(ValueError, match='Exactly one of filename or fileobj must be provided'):
        save_parquet(test_df, filename='test.parquet', fileobj=io.BytesIO())
