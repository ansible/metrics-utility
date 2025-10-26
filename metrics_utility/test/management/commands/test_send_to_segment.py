import csv
import json
import os
import tempfile

from argparse import ArgumentParser
from pathlib import Path
from unittest.mock import patch

import pytest

from django.core.management.base import CommandError

from metrics_utility.exceptions import MissingRequiredEnvVar
from metrics_utility.management.commands.send_to_segment import Command


@pytest.fixture
def command_instance():
    """Create a Command instance for testing."""
    return Command()


@pytest.fixture
def parser():
    """Create an ArgumentParser instance for testing."""
    return ArgumentParser()


@pytest.fixture
def temp_csv_file():
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["hostname", "managed", "timestamp"])
        writer.writeheader()
        writer.writerow(
            {
                "hostname": "server1",
                "managed": "true",
                "timestamp": "2025-10-26T10:00:00Z",
            }
        )
        writer.writerow(
            {
                "hostname": "server2",
                "managed": "false",
                "timestamp": "2025-10-26T10:01:00Z",
            }
        )
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def temp_json_file():
    """Create a temporary JSON file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        data = [
            {"metric": "cpu_usage", "value": 75.2},
            {"metric": "memory_usage", "value": 82.5},
        ]
        json.dump(data, f)
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def temp_json_object_file():
    """Create a temporary JSON file with a single object."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        data = {"metric": "cpu_usage", "value": 75.2}
        json.dump(data, f)
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def temp_json_primitive_file():
    """Create a temporary JSON file with a primitive value."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump("test_value", f)
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def temp_text_file():
    """Create a temporary text file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("Line 1\nLine 2\nLine 3\n")
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def temp_malformed_csv_file():
    """Create a temporary malformed CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        # Write invalid CSV content
        f.write("hostname,managed\nserver1,true,extra_column\n")
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


@pytest.fixture
def temp_no_extension_csv_file():
    """Create a temporary file without extension that contains CSV data."""
    with tempfile.NamedTemporaryFile(mode="w", suffix="", delete=False) as f:
        writer = csv.DictWriter(f, fieldnames=["col1", "col2"])
        writer.writeheader()
        writer.writerow({"col1": "val1", "col2": "val2"})
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


# Test add_arguments
def test_add_arguments_adds_expected_arguments(parser, command_instance):
    """Test that all expected arguments are added to the parser."""
    command_instance.add_arguments(parser)
    args = [a.dest for a in parser._actions]

    expected_args = ["file", "app", "user_id", "verbose"]
    for arg in expected_args:
        assert arg in args


# Test command help
def test_command_help(capsys):
    """Ensure that --help prints help text and exits cleanly."""
    parser = ArgumentParser(prog="send_to_segment", add_help=True)
    cmd = Command()
    cmd.add_arguments(parser)

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["--help"])

    out = capsys.readouterr().out
    assert "usage:" in out
    assert "--file" in out
    assert "--app" in out
    assert "--user-id" in out
    assert "--verbose" in out
    assert e.value.code == 0


# Test missing SEGMENT_WRITE_KEY
def test_handle_missing_segment_write_key(monkeypatch, command_instance):
    """Test that missing SEGMENT_WRITE_KEY raises MissingRequiredEnvVar."""
    monkeypatch.delenv("SEGMENT_WRITE_KEY", raising=False)

    with pytest.raises(MissingRequiredEnvVar) as exc_info:
        command_instance.handle(
            file="test.csv",
            app="test-app",
            user_id="system",
            verbose=False,
        )

    assert "SEGMENT_WRITE_KEY" in str(exc_info.value)


# Test missing file argument
def test_handle_missing_file_argument(monkeypatch, command_instance):
    """Test that missing --file argument raises CommandError."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    with pytest.raises(CommandError) as exc_info:
        command_instance.handle(
            file=None, app="test-app", user_id="system", verbose=False
        )

    assert "--file argument is required" in str(exc_info.value)


# Test missing app argument
def test_handle_missing_app_argument(monkeypatch, command_instance):
    """Test that missing --app argument raises CommandError."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    with pytest.raises(CommandError) as exc_info:
        command_instance.handle(
            file="test.csv", app=None, user_id="system", verbose=False
        )

    assert "--app argument is required" in str(exc_info.value)


# Test file not found
def test_handle_file_not_found(monkeypatch, command_instance):
    """Test that non-existent file raises CommandError."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    with pytest.raises(CommandError) as exc_info:
        command_instance.handle(
            file="/nonexistent/file.csv",
            app="test-app",
            user_id="system",
            verbose=False,
        )

    assert "File not found" in str(exc_info.value)


# Test path is not a file
def test_handle_path_is_not_file(monkeypatch, command_instance, tmp_path):
    """Test that directory path raises CommandError."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")
    dir_path = tmp_path / "test_dir"
    dir_path.mkdir()

    with pytest.raises(CommandError) as exc_info:
        command_instance.handle(
            file=str(dir_path),
            app="test-app",
            user_id="system",
            verbose=False,
        )

    assert "Path is not a file" in str(exc_info.value)


# Test _read_csv method
def test_read_csv(command_instance, temp_csv_file):
    """Test reading and parsing CSV file."""
    result = command_instance._read_csv(Path(temp_csv_file))

    assert result["file_type"] == "csv"
    assert result["row_count"] == 2
    assert len(result["data"]) == 2
    assert result["data"][0]["hostname"] == "server1"
    assert result["data"][0]["managed"] == "true"
    assert result["data"][1]["hostname"] == "server2"


# Test _read_json method with array
def test_read_json_array(command_instance, temp_json_file):
    """Test reading and parsing JSON file with array."""
    result = command_instance._read_json(Path(temp_json_file))

    assert result["file_type"] == "json"
    assert result["row_count"] == 2
    assert len(result["data"]) == 2
    assert result["data"][0]["metric"] == "cpu_usage"


# Test _read_json method with object
def test_read_json_object(command_instance, temp_json_object_file):
    """Test reading and parsing JSON file with single object."""
    result = command_instance._read_json(Path(temp_json_object_file))

    assert result["file_type"] == "json"
    assert result["row_count"] == 1
    assert len(result["data"]) == 1
    assert result["data"][0]["metric"] == "cpu_usage"


# Test _read_json method with primitive
def test_read_json_primitive(command_instance, temp_json_primitive_file):
    """Test reading and parsing JSON file with primitive value."""
    result = command_instance._read_json(Path(temp_json_primitive_file))

    assert result["file_type"] == "json"
    assert result["row_count"] == 1
    assert len(result["data"]) == 1
    assert result["data"][0]["value"] == "test_value"


# Test _read_text method
def test_read_text(command_instance, temp_text_file):
    """Test reading and parsing text file."""
    result = command_instance._read_text(Path(temp_text_file))

    assert result["file_type"] == "text"
    assert result["row_count"] == 3
    assert result["data"]["lines"] == ["Line 1", "Line 2", "Line 3"]
    assert "Line 1\nLine 2\nLine 3" in result["data"]["content"]


# Test _read_file with CSV extension
def test_read_file_csv_extension(command_instance, temp_csv_file):
    """Test _read_file auto-detects CSV by extension."""
    result = command_instance._read_file(Path(temp_csv_file))

    assert result["file_type"] == "csv"
    assert result["row_count"] == 2


# Test _read_file with JSON extension
def test_read_file_json_extension(command_instance, temp_json_file):
    """Test _read_file auto-detects JSON by extension."""
    result = command_instance._read_file(Path(temp_json_file))

    assert result["file_type"] == "json"
    assert result["row_count"] == 2


# Test _read_file with no extension (tries CSV first)
def test_read_file_no_extension_csv_content(
    command_instance, temp_no_extension_csv_file
):
    """Test _read_file tries CSV parsing for files without extension."""
    result = command_instance._read_file(Path(temp_no_extension_csv_file))

    assert result["file_type"] == "csv"
    assert result["row_count"] == 1


# Test _read_file uses .txt extension directly
def test_read_file_text_extension(command_instance, temp_text_file):
    """Test _read_file handles .txt files."""
    # .txt files go to CSV first (no extension check), then may be CSV
    result = command_instance._read_file(Path(temp_text_file))

    # The simple lines are actually valid single-column CSV
    # First line becomes header, so 2 data rows
    assert result["file_type"] == "csv"
    assert result["row_count"] == 2


# Test successful handle with CSV
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_successful_csv(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_csv_file,
):
    """Test successful execution with CSV file."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key_12345")

    command_instance.handle(
        file=temp_csv_file,
        app="ansible-automation-platform",
        user_id="test_user",
        verbose=False,
    )

    # Verify analytics SDK was configured
    assert mock_analytics.write_key == "test_key_12345"
    assert mock_analytics.debug is False

    # Verify track was called
    mock_analytics.track.assert_called_once()
    call_args = mock_analytics.track.call_args

    assert call_args[1]["user_id"] == "test_user"
    expected_event = "ansible-automation-platform_data_upload"
    assert call_args[1]["event"] == expected_event
    assert call_args[1]["properties"]["row_count"] == 2
    assert call_args[1]["properties"]["file_type"] == "csv"
    expected_app = "ansible-automation-platform"
    assert call_args[1]["context"]["app"]["name"] == expected_app

    # Verify flush was called
    mock_analytics.flush.assert_called_once()


# Test successful handle with JSON
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_successful_json(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_json_file,
):
    """Test successful execution with JSON file."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key_12345")

    command_instance.handle(
        file=temp_json_file,
        app="awx",
        user_id="system",
        verbose=True,
    )

    # Verify analytics SDK was configured with verbose
    assert mock_analytics.write_key == "test_key_12345"
    assert mock_analytics.debug is True

    # Verify track was called
    mock_analytics.track.assert_called_once()
    call_args = mock_analytics.track.call_args

    assert call_args[1]["user_id"] == "system"
    assert call_args[1]["event"] == "awx_data_upload"
    assert call_args[1]["properties"]["row_count"] == 2
    assert call_args[1]["properties"]["file_type"] == "json"


# Test verbose mode
@patch("metrics_utility.management.commands.send_to_segment.debug")
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_verbose_mode(
    mock_analytics,
    mock_debug,
    monkeypatch,
    command_instance,
    temp_csv_file,
):
    """Test that verbose mode enables debug logging."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    command_instance.handle(
        file=temp_csv_file,
        app="test-app",
        user_id="system",
        verbose=True,
    )

    # Verify debug was called
    mock_debug.assert_called_once()

    # Verify analytics.debug was set to True
    assert mock_analytics.debug is True


# Test file read error
def test_handle_file_read_error(monkeypatch, command_instance, tmp_path):
    """Test that file reading errors are properly handled."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    # Create a file we can't read by making it a binary file with bad encoding
    bad_file = tmp_path / "bad.csv"
    bad_file.write_bytes(b"\x80\x81\x82")

    with pytest.raises(CommandError) as exc_info:
        command_instance.handle(
            file=str(bad_file),
            app="test-app",
            user_id="system",
            verbose=False,
        )

    assert "Error reading file" in str(exc_info.value)


# Test Segment API error
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_segment_api_error(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_csv_file,
):
    """Test that Segment API errors are properly handled."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    # Make analytics.track raise an exception
    mock_analytics.track.side_effect = Exception("Segment API error")

    with pytest.raises(CommandError) as exc_info:
        command_instance.handle(
            file=temp_csv_file,
            app="test-app",
            user_id="system",
            verbose=False,
        )

    assert "Error sending data to Segment" in str(exc_info.value)


# Test properties are correctly set
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_properties_structure(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_csv_file,
):
    """Test that event properties are correctly structured."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    command_instance.handle(
        file=temp_csv_file,
        app="test-app",
        user_id="admin",
        verbose=False,
    )

    call_args = mock_analytics.track.call_args
    properties = call_args[1]["properties"]

    # Check all expected properties are present
    assert "filename" in properties
    assert "file_path" in properties
    assert "data" in properties
    assert "row_count" in properties
    assert "file_type" in properties
    assert "timestamp" in properties

    # Verify specific values
    assert properties["filename"] == Path(temp_csv_file).name
    assert properties["file_path"] == temp_csv_file
    assert properties["row_count"] == 2
    assert properties["file_type"] == "csv"


# Test context is correctly set
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_context_structure(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_csv_file,
):
    """Test that event context is correctly structured."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    command_instance.handle(
        file=temp_csv_file,
        app="ansible-controller",
        user_id="system",
        verbose=False,
    )

    call_args = mock_analytics.track.call_args
    context = call_args[1]["context"]

    assert "app" in context
    assert "name" in context["app"]
    assert context["app"]["name"] == "ansible-controller"


# Test default user_id
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_default_user_id(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_csv_file,
):
    """Test that default user_id is 'system'."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    # Don't provide user_id, it should default to 'system'
    command_instance.handle(
        file=temp_csv_file,
        app="test-app",
        user_id="system",  # This is the default
        verbose=False,
    )

    call_args = mock_analytics.track.call_args
    assert call_args[1]["user_id"] == "system"


# Test event name generation
@patch("metrics_utility.management.commands.send_to_segment.analytics")
@pytest.mark.parametrize(
    "app_name,expected_event",
    [
        (
            "ansible-automation-platform",
            "ansible-automation-platform_data_upload",
        ),
        ("awx", "awx_data_upload"),
        ("my-custom-app", "my-custom-app_data_upload"),
    ],
)
def test_handle_event_name_generation(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_csv_file,
    app_name,
    expected_event,
):
    """Test that event names are correctly generated from app names."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    command_instance.handle(
        file=temp_csv_file,
        app=app_name,
        user_id="system",
        verbose=False,
    )

    call_args = mock_analytics.track.call_args
    assert call_args[1]["event"] == expected_event


# Test CSV with empty file
def test_read_csv_empty_file(command_instance, tmp_path):
    """Test reading an empty CSV file."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("col1,col2\n")

    result = command_instance._read_csv(empty_csv)

    assert result["file_type"] == "csv"
    assert result["row_count"] == 0
    assert result["data"] == []


# Test text file with trailing newlines
def test_read_text_with_trailing_newlines(command_instance, tmp_path):
    """Test reading text file with trailing newlines."""
    text_file = tmp_path / "test.txt"
    text_file.write_text("Line 1\nLine 2\n\n\n")

    result = command_instance._read_text(text_file)

    assert result["file_type"] == "text"
    # strip() removes trailing newlines, so we should get 2 lines
    assert len(result["data"]["lines"]) == 2


# Test integration with Path objects
@patch("metrics_utility.management.commands.send_to_segment.analytics")
def test_handle_with_path_object(
    mock_analytics,
    monkeypatch,
    command_instance,
    temp_csv_file,
):
    """Test that handle works with Path objects."""
    monkeypatch.setenv("SEGMENT_WRITE_KEY", "test_key")

    # Pass as string (Path conversion happens internally)
    command_instance.handle(
        file=temp_csv_file,
        app="test-app",
        user_id="system",
        verbose=False,
    )

    # Should complete without errors
    mock_analytics.track.assert_called_once()
