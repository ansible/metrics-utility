from datetime import UTC, datetime
from unittest.mock import MagicMock

from metrics_utility.library.collectors.controller.credentials_service import credentials_service


def test_credentials_service_returns_managed_types_used_by_finished_jobs():
    """The billing collector retains its managed, finished-job behavior."""
    output = MagicMock()
    output.sql.return_value = 'projected'

    result = credentials_service(
        db=MagicMock(),
        since=datetime(2025, 6, 12, tzinfo=UTC),
        until=datetime(2025, 6, 14, tzinfo=UTC),
        output=output,
    ).gather()

    assert result == 'projected'
    query = output.sql.call_args.args[1]
    assert 'SELECT DISTINCT' in query
    assert 'main_credentialtype.name as credential_type' in query
    assert 'main_credentialtype.managed = true' in query
    assert 'main_unifiedjob.finished' in query
    assert 'COUNT(*) AS credential_count' not in query
    assert 'used_by_finished_job_count' not in query
