from unittest.mock import MagicMock

from metrics_utility.library.collectors.awx.inventory_counts import inventory_counts


def _make_mock_db():
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_db, mock_cursor


def test_inventory_counts_basic():
    """Test inventory_counts collector basic functionality."""
    mock_db = MagicMock()

    instance = inventory_counts(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_inventory_counts_with_data():
    """Test inventory_counts returns correct structure with inventories."""
    mock_db, mock_cursor = _make_mock_db()

    # Three sequential queries:
    # 1. Normal inventories
    # 2. Source details per inventory
    # 3. Smart inventories
    mock_cursor.fetchall.side_effect = [
        # Normal inventories: (id, name, kind, num_hosts, num_sources)
        [
            (1, 'Production', '', 50, 2),
            (2, 'Staging', '', 10, 1),
        ],
        # Source details: (inv_id, name, source, num_hosts)
        [
            (1, 'AWS Source', 'ec2', 30),
            (1, 'GCE Source', 'gce', 20),
            (2, 'Manual Source', '', 10),
        ],
        # Smart inventories: (id, name, kind, num_hosts)
        [
            (3, 'Smart Inv', 'smart', 25),
        ],
    ]

    instance = inventory_counts(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)

    # Normal inventory
    assert 1 in result
    assert result[1]['name'] == 'Production'
    assert result[1]['kind'] == ''
    assert result[1]['hosts'] == 50
    assert result[1]['sources'] == 2
    assert len(result[1]['source_list']) == 2
    assert result[1]['source_list'][0] == {'name': 'AWS Source', 'source': 'ec2', 'num_hosts': 30}
    assert result[1]['source_list'][1] == {'name': 'GCE Source', 'source': 'gce', 'num_hosts': 20}

    assert 2 in result
    assert result[2]['name'] == 'Staging'
    assert len(result[2]['source_list']) == 1

    # Smart inventory
    assert 3 in result
    assert result[3]['kind'] == 'smart'
    assert result[3]['hosts'] == 25
    assert result[3]['sources'] == 0
    assert result[3]['source_list'] == []


def test_inventory_counts_empty():
    """Test inventory_counts with no inventories."""
    mock_db, mock_cursor = _make_mock_db()

    # No normal inventories, no smart inventories
    mock_cursor.fetchall.side_effect = [
        [],  # Normal inventories
        # Source details query is skipped when counts is empty
        [],  # Smart inventories
    ]

    instance = inventory_counts(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)
    assert len(result) == 0


def test_inventory_counts_source_for_unknown_inventory():
    """Test that source details for inventories not in counts are ignored."""
    mock_db, mock_cursor = _make_mock_db()

    mock_cursor.fetchall.side_effect = [
        # Normal inventories
        [(1, 'Production', '', 50, 1)],
        # Source details includes an inventory_id (999) not in counts
        [
            (1, 'AWS Source', 'ec2', 30),
            (999, 'Orphan Source', 'manual', 5),
        ],
        # Smart inventories
        [],
    ]

    instance = inventory_counts(db=mock_db)
    result = instance.gather()

    assert 1 in result
    assert len(result[1]['source_list']) == 1
    assert 999 not in result


def test_inventory_counts_only_smart():
    """Test inventory_counts with only smart inventories."""
    mock_db, mock_cursor = _make_mock_db()

    mock_cursor.fetchall.side_effect = [
        [],  # No normal inventories
        # Source details query skipped
        [(5, 'My Smart', 'smart', 100)],  # Smart inventories
    ]

    instance = inventory_counts(db=mock_db)
    result = instance.gather()

    assert len(result) == 1
    assert 5 in result
    assert result[5]['kind'] == 'smart'
    assert result[5]['sources'] == 0
    assert result[5]['source_list'] == []
