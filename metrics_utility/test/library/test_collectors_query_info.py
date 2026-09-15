from metrics_utility.library.collectors.awx.query_info import query_info


def test_query_info_basic():
    """Test query_info collector basic functionality."""
    instance = query_info(since='2024-01-01', until='2024-01-02')

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['since'] == '2024-01-01'
    assert instance.kwargs['until'] == '2024-01-02'


def test_query_info_output_structure():
    """Test query_info returns expected dict structure."""
    instance = query_info(since='2024-01-01', until='2024-01-02')
    result = instance.gather()

    assert isinstance(result, dict)
    assert 'last_run' in result
    assert 'current_time' in result
    assert 'collection_type' in result


def test_query_info_values():
    """Test query_info returns correct values."""
    instance = query_info(since='2024-06-01', until='2024-06-02')
    result = instance.gather()

    assert result['last_run'] == '2024-06-01'
    assert result['current_time'] == '2024-06-02'


def test_query_info_default_collection_type():
    """Test query_info defaults collection_type to 'metrics-service'."""
    instance = query_info()
    result = instance.gather()

    assert result['collection_type'] == 'metrics-service'


def test_query_info_custom_collection_type():
    """Test query_info accepts custom collection_type."""
    instance = query_info(collection_type='manual')
    result = instance.gather()

    assert result['collection_type'] == 'manual'


def test_query_info_none_defaults():
    """Test query_info handles None since/until."""
    instance = query_info()
    result = instance.gather()

    assert result['last_run'] == 'None'
    assert result['current_time'] == 'None'
