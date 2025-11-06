"""
Unit tests for helper functions.
"""

import pytest

from metrics_utility.anonymized_rollups.helpers import sanitize_json


def test_sanitize_json_with_nan():
    """Test that NaN values are replaced with None."""
    obj = {'value': float('nan')}
    result = sanitize_json(obj)
    assert result == {'value': None}


def test_sanitize_json_with_infinity():
    """Test that infinity values are replaced with None."""
    obj = {'pos_inf': float('inf'), 'neg_inf': float('-inf')}
    result = sanitize_json(obj)
    assert result == {'pos_inf': None, 'neg_inf': None}


def test_sanitize_json_with_list():
    """Test sanitization of lists with NaN and infinity."""
    obj = [1, float('nan'), 3, float('inf'), 5]
    result = sanitize_json(obj)
    assert result == [1, None, 3, None, 5]


def test_sanitize_json_with_nested_dict():
    """Test sanitization of nested dictionaries."""
    obj = {'nested': {'value': float('nan'), 'normal': 42}, 'top_level': float('inf')}
    result = sanitize_json(obj)
    assert result == {'nested': {'value': None, 'normal': 42}, 'top_level': None}


def test_sanitize_json_with_nested_list():
    """Test sanitization of nested lists."""
    obj = {'data': [{'value': float('nan')}, {'value': 42}]}
    result = sanitize_json(obj)
    assert result == {'data': [{'value': None}, {'value': 42}]}


def test_sanitize_json_with_tuple():
    """Test that tuples are converted to lists and sanitized."""
    obj = (1, float('nan'), 3)
    result = sanitize_json(obj)
    assert result == [1, None, 3]


def test_sanitize_json_with_normal_values():
    """Test that normal values are not modified."""
    obj = {
        'string': 'hello',
        'int': 42,
        'float': 3.14,
        'bool': True,
        'none': None,
        'list': [1, 2, 3],
        'dict': {'a': 1, 'b': 2},
    }
    result = sanitize_json(obj)
    assert result == obj


def test_sanitize_json_with_complex_structure():
    """Test sanitization of a complex structure similar to anonymized rollups."""
    obj = {
        'jobs': [
            {'job_template_name': 'T1', 'number_of_jobs_executed': 5, 'avg_duration': float('nan')},
            {'job_template_name': 'T2', 'number_of_jobs_executed': 3, 'avg_duration': 120.5},
        ],
        'execution_environments': {'total_EE': 10, 'ratio': float('inf')},
        'events_modules': {
            'modules_used_to_automate_total': 7,
            'module_stats': [{'module_name': 'debug', 'count': float('nan')}, {'module_name': 'copy', 'count': 42}],
        },
    }

    result = sanitize_json(obj)

    # Verify NaN and inf are replaced
    assert result['jobs'][0]['avg_duration'] is None
    assert result['jobs'][1]['avg_duration'] == pytest.approx(120.5)
    assert result['execution_environments']['ratio'] is None
    assert result['events_modules']['module_stats'][0]['count'] is None
    assert result['events_modules']['module_stats'][1]['count'] == 42


def test_sanitize_json_json_serializable():
    """Test that the result can be serialized to JSON."""
    import json

    obj = {
        'value1': float('nan'),
        'value2': float('inf'),
        'value3': float('-inf'),
        'nested': {'value4': float('nan')},
        'list': [1, float('nan'), 3],
    }

    result = sanitize_json(obj)

    # This should not raise an exception
    json_string = json.dumps(result)
    assert json_string is not None

    # Verify we can parse it back
    parsed = json.loads(json_string)
    assert parsed['value1'] is None
    assert parsed['value2'] is None
    assert parsed['value3'] is None
