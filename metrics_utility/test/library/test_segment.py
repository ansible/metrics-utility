import csv
import json

from pathlib import Path
from unittest.mock import patch

import pytest

from metrics_utility.library.segment import (
    SegmentConfigurationError,
    SegmentDataError,
    SegmentError,
    SegmentSender,
    send_csv_file,
    send_data,
    send_json_file,
)


# Fixtures


@pytest.fixture
def temp_csv_file(tmp_path):
    """Create a temporary CSV file for testing."""
    csv_file = tmp_path / 'test.csv'
    with open(csv_file, 'w', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['col1', 'col2'])
        writer.writeheader()
        writer.writerow({'col1': 'val1', 'col2': 'val2'})
        writer.writerow({'col1': 'val3', 'col2': 'val4'})
    return csv_file


@pytest.fixture
def temp_json_file(tmp_path):
    """Create a temporary JSON file with array for testing."""
    json_file = tmp_path / 'test.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump([{'key': 'value1'}, {'key': 'value2'}], f)
    return json_file


@pytest.fixture
def temp_json_object_file(tmp_path):
    """Create a temporary JSON file with single object for testing."""
    json_file = tmp_path / 'test_obj.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({'key': 'value'}, f)
    return json_file


@pytest.fixture
def temp_text_file(tmp_path):
    """Create a temporary text file for testing."""
    text_file = tmp_path / 'test.txt'
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write('Line 1\nLine 2\nLine 3')
    return text_file


# Test Exception Classes


def test_segment_error_inheritance():
    """Test that SegmentError is an Exception."""
    assert issubclass(SegmentError, Exception)


def test_segment_configuration_error_inheritance():
    """Test that SegmentConfigurationError inherits from SegmentError."""
    assert issubclass(SegmentConfigurationError, SegmentError)


def test_segment_data_error_inheritance():
    """Test that SegmentDataError inherits from SegmentError."""
    assert issubclass(SegmentDataError, SegmentError)


# Test SegmentSender.__init__


def test_segment_sender_init_success():
    """Test successful initialization of SegmentSender."""
    sender = SegmentSender(write_key='test_key')
    assert sender.write_key == 'test_key'
    assert sender.debug is False


def test_segment_sender_init_with_debug():
    """Test initialization with debug enabled."""
    sender = SegmentSender(write_key='test_key', debug=True)
    assert sender.debug is True


def test_segment_sender_init_missing_write_key():
    """Test that missing write_key raises error."""
    with pytest.raises(SegmentConfigurationError) as exc_info:
        SegmentSender(write_key='')
    assert 'write_key is required' in str(exc_info.value)


def test_segment_sender_init_none_write_key():
    """Test that None write_key raises error."""
    with pytest.raises(SegmentConfigurationError) as exc_info:
        SegmentSender(write_key=None)
    assert 'write_key is required' in str(exc_info.value)


# Test SegmentSender.send with list data


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_list_success(mock_analytics):
    """Test sending list of dictionaries."""
    sender = SegmentSender(write_key='test_key')

    result = sender.send(
        data=[{'metric': 'cpu', 'value': 75}],
        app='test-app',
        user_id='test-user',
    )

    assert result['success'] is True
    assert result['event_name'] == 'test-app_data_upload'
    assert result['row_count'] == 1
    assert 'Successfully sent' in result['message']

    mock_analytics.track.assert_called_once()
    mock_analytics.flush.assert_called_once()


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_configures_analytics(mock_analytics):
    """Test that send configures analytics SDK."""
    sender = SegmentSender(write_key='test_key_123', debug=True)

    sender.send(data=[{'test': 'data'}], app='test-app')

    assert mock_analytics.write_key == 'test_key_123'
    assert mock_analytics.debug is True


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_event_structure(mock_analytics):
    """Test that event has correct structure."""
    sender = SegmentSender(write_key='test_key')

    sender.send(
        data=[{'metric': 'cpu', 'value': 75}],
        app='test-app',
        user_id='admin',
    )

    call_args = mock_analytics.track.call_args
    assert call_args[1]['user_id'] == 'admin'
    assert call_args[1]['event'] == 'test-app_data_upload'
    assert 'data' in call_args[1]['properties']
    assert 'row_count' in call_args[1]['properties']
    assert 'data_type' in call_args[1]['properties']
    assert 'timestamp' in call_args[1]['properties']
    assert call_args[1]['context']['app']['name'] == 'test-app'


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_additional_properties(mock_analytics):
    """Test sending with additional properties."""
    sender = SegmentSender(write_key='test_key')

    sender.send(
        data=[{'test': 'data'}],
        app='test-app',
        additional_properties={'source': 'api', 'version': '1.0'},
    )

    call_args = mock_analytics.track.call_args
    properties = call_args[1]['properties']
    assert properties['source'] == 'api'
    assert properties['version'] == '1.0'


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_single_dict(mock_analytics):
    """Test sending single dictionary."""
    sender = SegmentSender(write_key='test_key')

    result = sender.send(data={'metric': 'cpu'}, app='test-app')

    assert result['success'] is True
    assert result['row_count'] == 1


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_csv_file(mock_analytics, temp_csv_file):
    """Test sending CSV file."""
    sender = SegmentSender(write_key='test_key')

    result = sender.send(data=temp_csv_file, app='test-app')

    assert result['success'] is True
    assert result['row_count'] == 2


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_json_file(mock_analytics, temp_json_file):
    """Test sending JSON file."""
    sender = SegmentSender(write_key='test_key')

    result = sender.send(data=temp_json_file, app='test-app')

    assert result['success'] is True
    assert result['row_count'] == 2


def test_segment_sender_send_missing_app():
    """Test that missing app raises error."""
    sender = SegmentSender(write_key='test_key')

    with pytest.raises(SegmentConfigurationError) as exc_info:
        sender.send(data=[{'test': 'data'}], app='')
    assert 'app parameter is required' in str(exc_info.value)


@patch('metrics_utility.library.segment.analytics')
def test_segment_sender_send_api_error(mock_analytics):
    """Test handling of Segment API errors."""
    sender = SegmentSender(write_key='test_key')
    mock_analytics.track.side_effect = Exception('API Error')

    with pytest.raises(SegmentDataError) as exc_info:
        sender.send(data=[{'test': 'data'}], app='test-app')

    assert 'Failed to send data to Segment' in str(exc_info.value)


# Test SegmentSender._process_data


def test_process_data_list():
    """Test processing list of dictionaries."""
    sender = SegmentSender(write_key='test_key')
    data = [{'key': 'value1'}, {'key': 'value2'}]

    result = sender._process_data(data)

    assert result['data'] == data
    assert result['row_count'] == 2
    assert result['data_type'] == 'list'


def test_process_data_single_dict():
    """Test processing single dictionary."""
    sender = SegmentSender(write_key='test_key')
    data = {'key': 'value'}

    result = sender._process_data(data)

    assert result['data'] == [data]
    assert result['row_count'] == 1
    assert result['data_type'] == 'dict'


def test_process_data_unsupported_type():
    """Test that unsupported data type raises error."""
    sender = SegmentSender(write_key='test_key')

    with pytest.raises(SegmentDataError) as exc_info:
        sender._process_data(12345)  # Integer not supported

    assert 'Unsupported data type' in str(exc_info.value)


# Test SegmentSender._read_file


def test_read_file_csv(temp_csv_file):
    """Test reading CSV file."""
    sender = SegmentSender(write_key='test_key')

    result = sender._read_file(temp_csv_file)

    assert result['data_type'] == 'csv'
    assert result['row_count'] == 2
    assert result['data'][0]['col1'] == 'val1'


def test_read_file_json(temp_json_file):
    """Test reading JSON file."""
    sender = SegmentSender(write_key='test_key')

    result = sender._read_file(temp_json_file)

    assert result['data_type'] == 'json'
    assert result['row_count'] == 2


def test_read_file_not_found():
    """Test reading non-existent file."""
    sender = SegmentSender(write_key='test_key')

    with pytest.raises(SegmentDataError) as exc_info:
        sender._read_file(Path('/nonexistent/file.csv'))

    assert 'File not found' in str(exc_info.value)


def test_read_file_not_a_file(tmp_path):
    """Test reading directory instead of file."""
    sender = SegmentSender(write_key='test_key')

    with pytest.raises(SegmentDataError) as exc_info:
        sender._read_file(tmp_path)

    assert 'Path is not a file' in str(exc_info.value)


# Test SegmentSender._read_csv


def test_read_csv_success(temp_csv_file):
    """Test reading CSV file."""
    sender = SegmentSender(write_key='test_key')

    result = sender._read_csv(temp_csv_file)

    assert result['data_type'] == 'csv'
    assert result['row_count'] == 2
    assert len(result['data']) == 2
    assert result['data'][0]['col1'] == 'val1'


def test_read_csv_empty(tmp_path):
    """Test reading empty CSV file."""
    csv_file = tmp_path / 'empty.csv'
    with open(csv_file, 'w') as f:
        f.write('col1,col2\n')

    sender = SegmentSender(write_key='test_key')
    result = sender._read_csv(csv_file)

    assert result['row_count'] == 0
    assert result['data'] == []


# Test SegmentSender._read_json


def test_read_json_array(temp_json_file):
    """Test reading JSON array."""
    sender = SegmentSender(write_key='test_key')

    result = sender._read_json(temp_json_file)

    assert result['data_type'] == 'json'
    assert result['row_count'] == 2


def test_read_json_object(temp_json_object_file):
    """Test reading JSON object."""
    sender = SegmentSender(write_key='test_key')

    result = sender._read_json(temp_json_object_file)

    assert result['data_type'] == 'json'
    assert result['row_count'] == 1
    assert result['data'][0]['key'] == 'value'


def test_read_json_primitive(tmp_path):
    """Test reading JSON primitive value."""
    json_file = tmp_path / 'primitive.json'
    with open(json_file, 'w') as f:
        json.dump('test_value', f)

    sender = SegmentSender(write_key='test_key')
    result = sender._read_json(json_file)

    assert result['row_count'] == 1
    assert result['data'][0]['value'] == 'test_value'


# Test SegmentSender._read_text


def test_read_text_success(temp_text_file):
    """Test reading text file."""
    sender = SegmentSender(write_key='test_key')

    result = sender._read_text(temp_text_file)

    assert result['data_type'] == 'text'
    assert result['row_count'] == 3
    assert result['data']['lines'] == ['Line 1', 'Line 2', 'Line 3']
    assert 'Line 1' in result['data']['content']


def test_read_text_with_trailing_newlines(tmp_path):
    """Test reading text with trailing newlines."""
    text_file = tmp_path / 'trailing.txt'
    with open(text_file, 'w') as f:
        f.write('Line 1\nLine 2\n\n\n')

    sender = SegmentSender(write_key='test_key')
    result = sender._read_text(text_file)

    # strip() removes trailing newlines
    assert len(result['data']['lines']) == 2


# Test send_data function


@patch('metrics_utility.library.segment.analytics')
def test_send_data_function_success(mock_analytics):
    """Test send_data function."""
    result = send_data(
        data=[{'test': 'data'}],
        app='test-app',
        write_key='test_key',
        user_id='test-user',
    )

    assert result['success'] is True
    assert result['event_name'] == 'test-app_data_upload'
    mock_analytics.track.assert_called_once()


@patch('metrics_utility.library.segment.analytics')
def test_send_data_with_additional_properties(mock_analytics):
    """Test send_data with additional properties."""
    send_data(
        data=[{'test': 'data'}],
        app='test-app',
        write_key='test_key',
        additional_properties={'source': 'api'},
    )

    call_args = mock_analytics.track.call_args
    assert call_args[1]['properties']['source'] == 'api'


@patch('metrics_utility.library.segment.analytics')
def test_send_data_with_debug(mock_analytics):
    """Test send_data with debug enabled."""
    send_data(
        data=[{'test': 'data'}],
        app='test-app',
        write_key='test_key',
        debug=True,
    )

    assert mock_analytics.debug is True


def test_send_data_invalid_config():
    """Test send_data with invalid configuration."""
    with pytest.raises(SegmentConfigurationError):
        send_data(
            data=[{'test': 'data'}],
            app='test-app',
            write_key='',  # Invalid
        )


# Test send_csv_file function


@patch('metrics_utility.library.segment.analytics')
def test_send_csv_file_success(mock_analytics, temp_csv_file):
    """Test send_csv_file function."""
    result = send_csv_file(file_path=temp_csv_file, app='test-app', write_key='test_key')

    assert result['success'] is True
    assert result['row_count'] == 2
    mock_analytics.track.assert_called_once()


@patch('metrics_utility.library.segment.analytics')
def test_send_csv_file_with_str_path(mock_analytics, temp_csv_file):
    """Test send_csv_file with string path."""
    result = send_csv_file(file_path=str(temp_csv_file), app='test-app', write_key='test_key')

    assert result['success'] is True


@patch('metrics_utility.library.segment.analytics')
def test_send_csv_file_with_user_id(mock_analytics, temp_csv_file):
    """Test send_csv_file with custom user_id."""
    send_csv_file(
        file_path=temp_csv_file,
        app='test-app',
        write_key='test_key',
        user_id='custom-user',
    )

    call_args = mock_analytics.track.call_args
    assert call_args[1]['user_id'] == 'custom-user'


# Test send_json_file function


@patch('metrics_utility.library.segment.analytics')
def test_send_json_file_success(mock_analytics, temp_json_file):
    """Test send_json_file function."""
    result = send_json_file(file_path=temp_json_file, app='test-app', write_key='test_key')

    assert result['success'] is True
    assert result['row_count'] == 2
    mock_analytics.track.assert_called_once()


@patch('metrics_utility.library.segment.analytics')
def test_send_json_file_with_str_path(mock_analytics, temp_json_file):
    """Test send_json_file with string path."""
    result = send_json_file(file_path=str(temp_json_file), app='test-app', write_key='test_key')

    assert result['success'] is True


@patch('metrics_utility.library.segment.analytics')
def test_send_json_file_with_debug(mock_analytics, temp_json_file):
    """Test send_json_file with debug enabled."""
    send_json_file(
        file_path=temp_json_file,
        app='test-app',
        write_key='test_key',
        debug=True,
    )

    assert mock_analytics.debug is True


# Test file extension detection


def test_read_file_csv_extension(temp_csv_file):
    """Test that .csv extension is detected."""
    sender = SegmentSender(write_key='test_key')
    result = sender._read_file(temp_csv_file)
    assert result['data_type'] == 'csv'


def test_read_file_json_extension(temp_json_file):
    """Test that .json extension is detected."""
    sender = SegmentSender(write_key='test_key')
    result = sender._read_file(temp_json_file)
    assert result['data_type'] == 'json'


def test_read_file_unknown_extension_tries_csv(tmp_path):
    """Test that unknown extension tries CSV first."""
    unknown_file = tmp_path / 'test.unknown'
    with open(unknown_file, 'w') as f:
        f.write('col1,col2\nval1,val2\n')

    sender = SegmentSender(write_key='test_key')
    result = sender._read_file(unknown_file)

    # Should successfully parse as CSV
    assert result['data_type'] == 'csv'


# Test error propagation


def test_process_data_file_error(tmp_path):
    """Test that file read errors are propagated."""
    sender = SegmentSender(write_key='test_key')
    bad_file = tmp_path / 'nonexistent.csv'

    with pytest.raises(SegmentDataError):
        sender._process_data(bad_file)


@patch('metrics_utility.library.segment.analytics')
def test_send_propagates_data_error(mock_analytics):
    """Test that data errors are propagated from send."""
    sender = SegmentSender(write_key='test_key')

    with pytest.raises(SegmentDataError):
        sender.send(data=Path('/nonexistent/file.csv'), app='test-app')


# Test default parameters


@patch('metrics_utility.library.segment.analytics')
def test_send_default_user_id(mock_analytics):
    """Test that default user_id is 'system'."""
    sender = SegmentSender(write_key='test_key')
    sender.send(data=[{'test': 'data'}], app='test-app')

    call_args = mock_analytics.track.call_args
    assert call_args[1]['user_id'] == 'system'


@patch('metrics_utility.library.segment.analytics')
def test_send_data_default_user_id(mock_analytics):
    """Test that send_data defaults to 'system' user_id."""
    send_data(data=[{'test': 'data'}], app='test-app', write_key='test_key')

    call_args = mock_analytics.track.call_args
    assert call_args[1]['user_id'] == 'system'


# Test event name generation


@patch('metrics_utility.library.segment.analytics')
@pytest.mark.parametrize(
    'app_name,expected_event',
    [
        (
            'ansible-automation-platform',
            'ansible-automation-platform_data_upload',
        ),
        ('awx', 'awx_data_upload'),
        ('my-custom-app', 'my-custom-app_data_upload'),
        ('test123', 'test123_data_upload'),
    ],
)
def test_event_name_generation(mock_analytics, app_name, expected_event):
    """Test that event names are correctly generated."""
    sender = SegmentSender(write_key='test_key')
    sender.send(data=[{'test': 'data'}], app=app_name)

    call_args = mock_analytics.track.call_args
    assert call_args[1]['event'] == expected_event


# Test timestamp generation


@patch('metrics_utility.library.segment.analytics')
def test_send_includes_timestamp(mock_analytics):
    """Test that timestamp is included in properties."""
    sender = SegmentSender(write_key='test_key')
    sender.send(data=[{'test': 'data'}], app='test-app')

    call_args = mock_analytics.track.call_args
    properties = call_args[1]['properties']
    assert 'timestamp' in properties
    assert 'T' in properties['timestamp']  # ISO format
    assert '+' in properties['timestamp']  # Timezone
