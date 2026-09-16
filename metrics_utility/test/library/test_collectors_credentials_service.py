from datetime import UTC, datetime
from unittest.mock import MagicMock

from metrics_utility.library.collectors.controller.credentials_service import credentials_service


def test_credentials_service_returns_union_fields_and_rows():
    """The Controller query exposes every credential type and both aggregates."""
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
    assert 'main_credentialtype.id' in query
    assert 'main_credentialtype.name' in query
    assert 'main_credentialtype.managed' in query
    assert 'COUNT(*) AS credential_count' in query
    assert 'COUNT(DISTINCT main_unifiedjob.id) AS used_by_finished_job_count' in query
    assert "main_unifiedjob.finished >= '2025-06-12T00:00:00+00:00'" in query
    assert "main_unifiedjob.finished < '2025-06-14T00:00:00+00:00'" in query
    assert 'credential_types.used_by_finished_job_count > 0' not in query
