from unittest.mock import MagicMock

from metrics_utility.library.collectors.awx.instance_info import (
    _get_control_task_impact,
    instance_info,
)


def test_instance_info_basic():
    """Test instance_info collector basic functionality."""
    mock_db = MagicMock()

    instance = instance_info(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_instance_info_output():
    """Test instance_info returns expected dict structure."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # First call: _get_control_task_impact query
    # Second call: main CTE query
    mock_cursor.fetchone.return_value = None  # No conf_setting row
    mock_cursor.fetchall.return_value = [
        ('uuid-1', 'node1.example.com', '4.5.0', 100, 4, 8192, True, True, 'hybrid', 30),
        ('uuid-2', 'node2.example.com', '4.5.0', 50, 2, 4096, False, True, 'execution', 10),
    ]

    instance = instance_info(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 2

    node1 = result['uuid-1']
    assert node1['uuid'] == 'uuid-1'
    assert node1['version'] == '4.5.0'
    assert node1['capacity'] == 100
    assert node1['cpu'] == 4
    assert node1['memory'] == 8192
    assert node1['managed_by_policy'] is True
    assert node1['enabled'] is True
    assert node1['consumed_capacity'] == 30
    assert node1['remaining_capacity'] == 70
    assert node1['node_type'] == 'hybrid'


def test_instance_info_remaining_capacity():
    """Test instance_info computes remaining_capacity correctly."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = [
        ('uuid-a', 'host-a', '4.5.0', 200, 8, 16384, True, True, 'execution', 75),
    ]

    instance = instance_info(db=mock_db)
    result = instance.gather()

    assert result['uuid-a']['remaining_capacity'] == 125  # 200 - 75


def test_instance_info_empty():
    """Test instance_info handles no instances."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []

    instance = instance_info(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 0


def test_get_control_task_impact_default():
    """Test _get_control_task_impact returns 1 when no conf_setting row."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = None

    result = _get_control_task_impact(mock_db)

    assert result == 1


def test_get_control_task_impact_from_db():
    """Test _get_control_task_impact reads value from conf_setting."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = ('5',)

    result = _get_control_task_impact(mock_db)

    assert result == 5


def test_get_control_task_impact_json_int():
    """Test _get_control_task_impact handles JSON integer value."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Value stored as JSON number (no quotes)
    mock_cursor.fetchone.return_value = ('3',)

    result = _get_control_task_impact(mock_db)

    assert result == 3


def test_get_control_task_impact_invalid_json():
    """Test _get_control_task_impact returns default on invalid JSON."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = ('not-valid-json{',)

    result = _get_control_task_impact(mock_db)

    assert result == 1


def test_get_control_task_impact_null_value():
    """Test _get_control_task_impact handles NULL value row."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = (None,)

    result = _get_control_task_impact(mock_db)

    assert result == 1


def test_instance_info_passes_ctrl_impact():
    """Test instance_info passes ctrl_impact param to query."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # _get_control_task_impact returns 1 (default)
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []

    instance = instance_info(db=mock_db)
    instance.gather()

    # Second execute call is the main query with params
    main_query_call = mock_cursor.execute.call_args_list[1]
    params = main_query_call[0][1]
    assert params == {'ctrl_impact': 1}


def test_instance_info_no_hostname_in_output():
    """Test instance_info excludes hostname from output dict."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = [
        ('uuid-1', 'node1.example.com', '4.5.0', 100, 4, 8192, True, True, 'hybrid', 0),
    ]

    instance = instance_info(db=mock_db)
    result = instance.gather()

    assert 'hostname' not in result['uuid-1']
