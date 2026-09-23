from datetime import date
from unittest.mock import MagicMock

import pandas as pd

from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_jobhost_summary_usage import (
    DataframeJobhostSummaryUsage,
)
from metrics_utility.metric_utils import DIRECT, INDIRECT


def test_indirect_hosts_are_grouped_by_hostname_and_organization():
    """Indirect rows share a host only within their organization."""
    direct_rows = pd.DataFrame(
        [
            {
                'created': '2026-09-23T12:00:00+00:00',
                'job_created': '2026-09-23T11:59:00+00:00',
                'host_name': 'direct-host',
                'changed': 0,
                'dark': 0,
                'failures': 0,
                'ok': 1,
                'skipped': 0,
                'ignored': 0,
                'rescued': 0,
                'job_remote_id': 99,
                'job_template_name': 'direct-node-test',
                'organization_name': 'Engineering',
            },
            {
                'created': '2026-09-23T12:01:00+00:00',
                'job_created': '2026-09-23T12:00:00+00:00',
                'host_name': 'direct-host',
                'changed': 0,
                'dark': 0,
                'failures': 0,
                'ok': 1,
                'skipped': 0,
                'ignored': 0,
                'rescued': 0,
                'job_remote_id': 99,
                'job_template_name': 'direct-node-test',
                'organization_name': 'Engineering',
            },
        ]
    )
    indirect_rows = pd.DataFrame(
        [
            {
                'created': '2026-09-23T12:00:00+00:00',
                'job_created': '2026-09-23T11:59:00+00:00',
                'host_name': hostname,
                'events': '[]',
                'task_runs': 1,
                'canonical_facts': '{}',
                'facts': '{}',
                'job_remote_id': job_remote_id,
                'job_template_name': 'indirect-node-test',
                'organization_name': organization_name,
            }
            for organization_name, hostname, job_remote_id in [
                ('Engineering', 'shared-host', 1),
                ('Engineering', 'shared-host', 1),
                ('Operations', 'shared-host', 1),
                ('Engineering', 'unique-host', 2),
            ]
        ]
    )
    extractor = MagicMock()
    extractor.iter_batches.return_value = iter(
        [
            {
                'job_host_summary': direct_rows,
                'main_indirectmanagednodeaudit': pd.DataFrame(),
                'config': {'install_uuid': 'install-uuid'},
            },
            {
                'job_host_summary': pd.DataFrame(),
                'main_indirectmanagednodeaudit': indirect_rows,
                'config': {'install_uuid': 'install-uuid'},
            },
        ]
    )
    dataframe = DataframeJobhostSummaryUsage(
        extractor=extractor,
        month=date(2026, 9, 1),
        extra_params={'since_date': date(2026, 9, 23), 'until_date': date(2026, 9, 23)},
    )

    result = dataframe.build_dataframe()

    indirect = result[result['managed_node_type'] == INDIRECT]
    direct = result[result['managed_node_type'] == DIRECT]

    assert len(indirect) == 3
    assert set(zip(indirect['organization_name'], indirect['host_name'])) == {
        ('Engineering', 'shared-host'),
        ('Engineering', 'unique-host'),
        ('Operations', 'shared-host'),
    }
    assert (
        indirect.loc[
            (indirect['organization_name'] == 'Engineering') & (indirect['host_name'] == 'shared-host'),
            'host_runs',
        ].item()
        == 2
    )

    assert len(direct) == 1
    assert direct.iloc[0]['host_name'] == 'direct-host'
    assert direct.iloc[0]['host_runs'] == 2
    assert direct.iloc[0]['task_runs'] == 2
