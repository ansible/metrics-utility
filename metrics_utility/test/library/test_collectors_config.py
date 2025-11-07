import json

from unittest.mock import MagicMock, patch

from metrics_utility.library.collectors.controller.config import (
    _datetime_hook,
    _get_controller_settings,
    _get_controller_version,
    _get_install_type,
    _version,
    config,
)


def test_version_existing_package():
    """Test _version function with an existing package."""
    # Test with a known package
    result = _version('pytest')
    assert result is not None
    assert isinstance(result, str)


def test_version_missing_package():
    """Test _version function with a non-existent package."""
    result = _version('nonexistent-package-xyz-123')
    assert result is None


def test_get_install_type_openshift():
    """Test _get_install_type detects OpenShift."""
    with patch.dict('os.environ', {'container': 'oci'}):
        result = _get_install_type()
        assert result == 'openshift'


def test_get_install_type_k8s():
    """Test _get_install_type detects Kubernetes."""
    with patch.dict('os.environ', {'KUBERNETES_SERVICE_PORT': '443'}, clear=True):
        result = _get_install_type()
        assert result == 'k8s'


def test_get_install_type_traditional():
    """Test _get_install_type detects traditional install."""
    with patch.dict('os.environ', {}, clear=True):
        result = _get_install_type()
        assert result == 'traditional'


def test_datetime_hook_with_datetime():
    """Test _datetime_hook converts datetime strings."""
    from django.utils.dateparse import parse_datetime

    input_dict = {'date_field': '2024-01-15T10:30:00Z'}

    result = _datetime_hook(input_dict)

    assert 'date_field' in result
    # Should be parsed as a datetime object
    assert result['date_field'] is not None
    expected = parse_datetime('2024-01-15T10:30:00Z')
    assert result['date_field'] == expected


def test_datetime_hook_with_non_datetime():
    """Test _datetime_hook handles non-string values (preserves them via TypeError)."""
    input_dict = {'number': 123, 'bool': True}

    result = _datetime_hook(input_dict)

    # Non-string values raise TypeError in parse_datetime, so they're preserved
    assert result['number'] == 123
    assert result['bool'] is True


def test_datetime_hook_with_non_datetime_string():
    """Test _datetime_hook with strings that aren't valid datetimes."""
    input_dict = {'text': 'hello'}

    result = _datetime_hook(input_dict)

    # parse_datetime returns None for invalid datetime strings
    assert result['text'] is None


def test_get_controller_settings():
    """Test _get_controller_settings retrieves settings from database."""
    # Create mock database
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Mock fetchall to return settings - use values that won't be affected by datetime parsing
    mock_cursor.fetchall.return_value = [
        ('LOG_AGGREGATOR_ENABLED', 'true'),
        ('PENDO_TRACKING_STATE', '"on"'),
    ]

    result = _get_controller_settings(mock_db, ['LOG_AGGREGATOR_ENABLED', 'PENDO_TRACKING_STATE'])

    assert 'LOG_AGGREGATOR_ENABLED' in result
    assert result['LOG_AGGREGATOR_ENABLED'] is True
    assert 'PENDO_TRACKING_STATE' in result


def test_get_controller_settings_with_null_value():
    """Test _get_controller_settings handles NULL values."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = [
        ('INSTALL_UUID', '{"value": "test-uuid"}'),
        ('LICENSE', None),  # NULL value
    ]

    result = _get_controller_settings(mock_db, ['INSTALL_UUID', 'LICENSE'])

    assert 'INSTALL_UUID' in result
    assert 'LICENSE' not in result  # NULL values should be skipped


def test_get_controller_version():
    """Test _get_controller_version retrieves version from database."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = ('4.5.0',)

    result = _get_controller_version(mock_db)

    assert result == '4.5.0'
    mock_cursor.execute.assert_called_once()


def test_get_controller_version_no_result():
    """Test _get_controller_version when no version is found."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = None

    result = _get_controller_version(mock_db)

    assert result is None


def test_get_controller_version_empty_string():
    """Test _get_controller_version when version is empty string."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.return_value = ('',)

    result = _get_controller_version(mock_db)

    assert result is None


def test_config_collector_basic():
    """Test config collector with basic database."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Mock settings query
    mock_cursor.fetchall.return_value = [
        ('INSTALL_UUID', '"test-install-uuid"'),
        ('SYSTEM_UUID', '"test-system-uuid"'),
        ('TOWER_URL_BASE', '"https://tower.example.com"'),
    ]

    # Mock version query
    mock_cursor.fetchone.return_value = ('4.5.0',)

    instance = config(db=mock_db, billing_provider_params={})
    result = instance.gather()

    # Should return a dict
    assert isinstance(result, dict)

    # Should have key fields
    assert 'install_uuid' in result
    assert 'instance_uuid' in result
    assert 'controller_url_base' in result
    assert 'metrics_utility_version' in result
    assert 'platform' in result

    assert result['install_uuid'] == 'test-install-uuid'
    assert result['instance_uuid'] == 'test-system-uuid'
    assert result['controller_url_base'] == 'https://tower.example.com'


def test_config_collector_with_license():
    """Test config collector with license information."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Use numeric and boolean values that won't be parsed by _datetime_hook
    license_data = {
        'instance_count': 100,
        'valid_key': True,
        'compliant': False,
        'trial': False,
    }

    mock_cursor.fetchall.return_value = [('LICENSE', json.dumps(license_data))]
    mock_cursor.fetchone.return_value = ('4.5.0',)

    instance = config(db=mock_db)
    result = instance.gather()

    assert result['total_licensed_instances'] == 100
    assert result['valid_key'] is True
    assert result['compliant'] is False
    assert result['trial'] is False


def test_config_collector_with_billing_provider_params():
    """Test config collector includes billing_provider_params."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = ('4.5.0',)

    billing_params = {'provider': 'aws', 'account': '123456'}

    instance = config(db=mock_db, billing_provider_params=billing_params)
    result = instance.gather()

    assert 'billing_provider_params' in result
    assert result['billing_provider_params'] == billing_params


def test_config_collector_platform_info():
    """Test config collector includes platform information."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = ('4.5.0',)

    instance = config(db=mock_db)
    result = instance.gather()

    assert 'platform' in result
    platform = result['platform']

    assert 'dist' in platform
    assert 'release' in platform
    assert 'system' in platform
    assert 'type' in platform


def test_config_collector_default_values():
    """Test config collector uses default values for missing license fields."""
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Empty license
    mock_cursor.fetchall.return_value = [('LICENSE', '{}')]
    mock_cursor.fetchone.return_value = ('4.5.0',)

    instance = config(db=mock_db)
    result = instance.gather()

    # Should have default values
    assert result['license_type'] == 'UNLICENSED'
    assert result['total_licensed_instances'] == 0
    assert result['free_instances'] == 0
    assert result['license_expiry'] == 0
