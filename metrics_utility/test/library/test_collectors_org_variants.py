from unittest.mock import MagicMock

from metrics_utility.library.collectors.controller import (
    cred_type_counts_org,
    credentials_service_org,
    inventory_counts_org,
    projects_by_scm_type_org,
    unified_jobs_org,
)
from metrics_utility.test.util import utcdt


def _query(collector_func, **kwargs):
    output = MagicMock()
    collector_func(db=MagicMock(), output=output, **kwargs).gather()
    output.sql.assert_called_once()
    return output.sql.call_args.args[1]


def test_unified_jobs_org_groups_status_and_type_with_finished_window():
    query = _query(unified_jobs_org, since=utcdt('2025-06-12'), until=utcdt('2025-06-14'))

    assert 'main_unifiedjob.organization_id' in query
    assert 'main_organization.name AS organization_name' in query
    assert 'django_content_type.model AS job_type' in query
    assert 'main_unifiedjob.status' in query
    assert 'COUNT(*) FILTER (WHERE main_unifiedjob.failed) AS failure_count' in query
    assert 'SUM(main_unifiedjob.elapsed)' in query
    assert 'main_unifiedjob.finished >=' in query
    assert 'main_unifiedjob.finished <' in query
    assert 'GROUP BY' in query


def test_projects_by_scm_type_org_uses_template_organization():
    query = _query(projects_by_scm_type_org)

    assert 'FROM main_project' in query
    assert 'main_unifiedjobtemplate.id = main_project.unifiedjobtemplate_ptr_id' in query
    assert 'main_unifiedjobtemplate.organization_id' in query
    assert "COALESCE(NULLIF(main_project.scm_type, ''), 'manual')" in query
    assert 'COUNT(*) AS project_count' in query
    assert 'GROUP BY' in query


def test_cred_type_counts_org_uses_credential_owning_organization():
    query = _query(cred_type_counts_org)

    assert 'main_credential.organization_id' in query
    assert 'main_organization.id = main_credential.organization_id' in query
    assert 'main_credentialtype.id AS credential_type_id' in query
    assert 'COUNT(main_credential.id) AS credential_count' in query
    assert 'GROUP BY' in query
    assert 'main_unifiedjob.organization_id' not in query


def test_credentials_service_org_attributes_usage_to_job_organization():
    query = _query(credentials_service_org, since=utcdt('2025-06-12'), until=utcdt('2025-06-14'))

    assert 'SELECT DISTINCT' in query
    assert 'main_unifiedjob.organization_id' in query
    assert 'main_organization.id = main_unifiedjob.organization_id' in query
    assert 'main_credentialtype.managed = true' in query
    assert 'main_unifiedjob.finished >=' in query
    assert 'main_unifiedjob.finished <' in query


def test_inventory_counts_org_aggregates_hosts_and_sources_before_joining():
    query = _query(inventory_counts_org)

    assert 'main_inventory.organization_id' in query
    assert 'main_organization.id = main_inventory.organization_id' in query
    assert "COALESCE(NULLIF(main_inventory.kind, ''), 'normal') AS inventory_kind" in query
    assert 'WITH host_counts AS' in query
    assert 'COUNT(DISTINCT id) AS host_count' in query
    assert 'source_counts AS' in query
    assert 'COUNT(DISTINCT unifiedjobtemplate_ptr_id) AS source_count' in query
    assert 'COUNT(main_inventory.id) AS inventory_count' in query
    assert 'SUM(host_counts.host_count)' in query
    assert 'SUM(source_counts.source_count)' in query
    assert 'LEFT JOIN host_counts' in query
    assert 'LEFT JOIN source_counts' in query
    assert "main_inventory.kind IN ('', 'smart')" in query
    assert 'organization_id IS NOT NULL' not in query
