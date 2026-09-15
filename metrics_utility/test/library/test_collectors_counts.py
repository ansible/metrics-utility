from unittest.mock import MagicMock

from metrics_utility.library.collectors.awx.counts import _MODEL_TABLES, counts


def _make_mock_db(fetchone_values, fetchall_values):
    """Build a mock DB where fetchone and fetchall return sequential values.

    counts does 11 model counts (fetchone each), then fetchall for
    inventory breakdown, then fetchone for unified_job, active_host_count,
    active_sessions, active_user_sessions, running_jobs, pending_jobs,
    database_connections.
    """
    mock_db = MagicMock()
    mock_cursor = MagicMock()
    mock_db.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_db.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_cursor.fetchone.side_effect = fetchone_values
    mock_cursor.fetchall.side_effect = fetchall_values

    return mock_db


def test_counts_basic():
    """Test counts collector basic functionality."""
    mock_db = MagicMock()

    instance = counts(db=mock_db)

    assert hasattr(instance, 'gather')
    assert hasattr(instance, 'kwargs')
    assert instance.kwargs['db'] == mock_db


def test_counts_output_structure():
    """Test counts returns all expected keys."""
    # 11 model counts via fetchone
    model_counts = [(i,) for i in range(len(_MODEL_TABLES))]
    # After model counts: unified_job, active_host_count, active_sessions,
    # active_user_sessions, running_jobs, pending_jobs, database_connections
    extra_fetchone = [(100,), (50,), (10,), (8,), (3,), (2,), (5,)]
    all_fetchone = model_counts + extra_fetchone

    # fetchall is called once for inventory breakdown
    all_fetchall = [[('normal', 4), ('smart', 2)]]

    mock_db = _make_mock_db(all_fetchone, all_fetchall)

    instance = counts(db=mock_db)
    result = instance.gather()

    assert isinstance(result, dict)

    # Model table keys
    for key, _table in _MODEL_TABLES:
        assert key in result

    # Other expected keys
    assert 'inventories' in result
    assert 'unified_job' in result
    assert 'active_host_count' in result
    assert 'active_sessions' in result
    assert 'active_user_sessions' in result
    assert 'active_anonymous_sessions' in result
    assert 'running_jobs' in result
    assert 'pending_jobs' in result
    assert 'database_connections' in result


def test_counts_model_counts():
    """Test counts returns correct model counts."""
    model_counts = [(10,), (5,), (20,), (3,), (15,), (8,), (12,), (4,), (100,), (7,), (6,)]
    extra_fetchone = [(200,), (80,), (10,), (8,), (3,), (2,), (5,)]
    all_fetchone = model_counts + extra_fetchone

    all_fetchall = [[('normal', 2), ('smart', 1)]]

    mock_db = _make_mock_db(all_fetchone, all_fetchall)

    instance = counts(db=mock_db)
    result = instance.gather()

    assert result['organization'] == 10
    assert result['team'] == 5
    assert result['user'] == 20
    assert result['inventory'] == 3
    assert result['credential'] == 15
    assert result['project'] == 8
    assert result['job_template'] == 12
    assert result['workflow_job_template'] == 4
    assert result['host'] == 100
    assert result['schedule'] == 7
    assert result['notification_template'] == 6


def test_counts_inventory_breakdown():
    """Test counts returns inventory kind breakdown."""
    model_counts = [(0,)] * len(_MODEL_TABLES)
    extra_fetchone = [(0,), (0,), (0,), (0,), (0,), (0,), (0,)]
    all_fetchone = model_counts + extra_fetchone

    all_fetchall = [[('normal', 10), ('smart', 3)]]

    mock_db = _make_mock_db(all_fetchone, all_fetchall)

    instance = counts(db=mock_db)
    result = instance.gather()

    assert result['inventories'] == {'normal': 10, 'smart': 3}


def test_counts_inventory_breakdown_defaults():
    """Test counts sets defaults for missing inventory kinds."""
    model_counts = [(0,)] * len(_MODEL_TABLES)
    extra_fetchone = [(0,), (0,), (0,), (0,), (0,), (0,), (0,)]
    all_fetchone = model_counts + extra_fetchone

    # Only 'normal' returned, 'smart' should default to 0
    all_fetchall = [[('normal', 5)]]

    mock_db = _make_mock_db(all_fetchone, all_fetchall)

    instance = counts(db=mock_db)
    result = instance.gather()

    assert result['inventories']['normal'] == 5
    assert result['inventories']['smart'] == 0


def test_counts_session_counts():
    """Test counts computes anonymous sessions correctly."""
    model_counts = [(0,)] * len(_MODEL_TABLES)
    # unified_job=0, active_host=0, active_sessions=10, user_sessions=7,
    # running=0, pending=0, db_conns=0
    extra_fetchone = [(0,), (0,), (10,), (7,), (0,), (0,), (0,)]
    all_fetchone = model_counts + extra_fetchone

    all_fetchall = [[]]

    mock_db = _make_mock_db(all_fetchone, all_fetchall)

    instance = counts(db=mock_db)
    result = instance.gather()

    assert result['active_sessions'] == 10
    assert result['active_user_sessions'] == 7
    assert result['active_anonymous_sessions'] == 3


def test_counts_execute_count():
    """Test counts makes the expected number of execute calls."""
    model_counts = [(0,)] * len(_MODEL_TABLES)
    extra_fetchone = [(0,), (0,), (0,), (0,), (0,), (0,), (0,)]
    all_fetchone = model_counts + extra_fetchone

    all_fetchall = [[]]

    mock_db = _make_mock_db(all_fetchone, all_fetchall)

    instance = counts(db=mock_db)
    instance.gather()

    mock_cursor = mock_db.cursor.return_value.__enter__.return_value

    # 11 model counts + inventory breakdown + unified_job + active_host_count
    # + active_sessions + active_user_sessions + running_jobs + pending_jobs
    # + database_connections = 19 execute calls
    assert mock_cursor.execute.call_count == 19
