"""Anonymized rollup for indirect managed node audit collector data."""

import json

import pandas as pd

from metrics_utility.anonymized_rollups.base_anonymized_rollup import BaseAnonymizedRollup
from metrics_utility.anonymized_rollups.helpers import sanitize_json
from metrics_utility.automation_controller_billing.dataframe_engine.dataframe_content_usage import (
    DataframeContentUsage,
)


GROUP_KEY_SEPARATOR = '||'


def _extract_collection_names(events_value):
    """Parse the events JSON column and return a set of collection names.

    Args:
        events_value: Raw events column value - could be a JSON string,
            a Python list, None, or NaN.

    Returns:
        Set of two-part collection name strings (e.g. ``{"azure.azcollection"}``).
    """
    if events_value is None:
        return set()

    if isinstance(events_value, float):
        return set()

    if isinstance(events_value, str):
        try:
            events_list = json.loads(events_value)
        except ValueError:
            return set()
    elif isinstance(events_value, list):
        events_list = events_value
    else:
        return set()

    collections = set()
    for fqcn in events_list:
        name = DataframeContentUsage.extract_collection_name(fqcn)
        if name is not None:
            collections.add(name)
    return collections


def _make_group_key(organization_name, collection_name):
    return f'{organization_name}{GROUP_KEY_SEPARATOR}{collection_name}'


def _aggregate_by_key(groups, key_field):
    """Aggregate host names across groups by a single field (org or collection)."""
    aggregated = {}
    for group in groups.values():
        key = group[key_field]
        if key not in aggregated:
            aggregated[key] = set()
        aggregated[key].update(group.get('host_names', []))
    return {k: {'host_names': sorted(v), 'host_count': len(v)} for k, v in sorted(aggregated.items())}


class IndirectManagedNodesAnonymizedRollup(BaseAnonymizedRollup):
    """Rollup processor for main_indirectmanagednodeaudit collector data.

    Groups indirect managed nodes by organization and Ansible collection,
    counting unique host names per group.
    """

    def __init__(self):
        super().__init__('indirect_managed_nodes')
        self.collector_names = ['main_indirectmanagednodeaudit']

    def prepare(self, dataframe):
        """Transform raw indirect node audit data into org/collection groups.

        Parses the events JSON column to extract Ansible collection names,
        then groups by (organization_name, collection_name) and counts unique
        host_name values per group. Uses host_name (not host_remote_id) because
        host_remote_id is always NULL for indirect managed nodes.

        Returns JSON-serializable dictionary with groups and total count.
        """
        dataframe = self._convert_id_columns_to_strings(dataframe)

        if dataframe.empty:
            return {'groups': {}, 'by_organizations': [], 'by_collections': [], 'indirect_nodes_total': 0}

        groups = {}
        all_host_names = set()

        for _, row in dataframe.iterrows():
            host_name = row.get('host_name')
            if pd.isna(host_name):
                continue

            host_name = str(host_name)
            org_name = str(row.get('organization_name', ''))
            all_host_names.add(host_name)

            collection_names = _extract_collection_names(row.get('events'))

            if not collection_names:
                collection_names = {'_no_collection'}

            for collection_name in collection_names:
                key = _make_group_key(org_name, collection_name)
                if key not in groups:
                    groups[key] = {
                        'organization_name': org_name,
                        'collection_name': collection_name,
                        'host_names': set(),
                    }
                groups[key]['host_names'].add(host_name)

        for group in groups.values():
            group['host_names'] = sorted(group['host_names'])
            group['host_count'] = len(group['host_names'])

        agg_by_org = _aggregate_by_key(groups, 'organization_name')
        agg_by_coll = _aggregate_by_key(groups, 'collection_name')

        by_organizations = [{'organization_name': k, 'host_count': v['host_count']} for k, v in agg_by_org.items()]
        by_collections = [{'collection_name': k, 'host_count': v['host_count']} for k, v in agg_by_coll.items()]

        return sanitize_json(
            {
                'groups': groups,
                'by_organizations': by_organizations,
                'by_collections': by_collections,
                'indirect_nodes_total': len(all_host_names),
            }
        )

    def base(self, data):
        """Return final rollup payload stripped of all PII.

        Strips host_names and organization_name (customer-identifiable).
        Collapses org-level groups into collection-level totals, re-deduplicating
        hosts that appear under the same collection in different orgs.
        Collection names are public Ansible Galaxy labels, not PII.

        Args:
            data: Accumulated dict from merge() calls, or None if no data.

        Returns:
            Dict with 'json' key containing the final rollup data.
        """
        if data is None:
            return {'json': {'indirect_nodes_total': 0, 'by_collection': []}}

        collection_hosts = {}
        for group in data.get('groups', {}).values():
            cname = group['collection_name']
            hosts = group.get('host_names', [])
            if cname not in collection_hosts:
                collection_hosts[cname] = set()
            collection_hosts[cname].update(hosts)

        by_collection = [{'collection_name': cname, 'host_count': len(hosts)} for cname, hosts in sorted(collection_hosts.items())]

        return {
            'json': {
                'indirect_nodes_total': data.get('indirect_nodes_total', 0),
                'by_collection': by_collection,
            }
        }

    def merge(self, data_all, data_new):
        """Merge two prepared batches by unioning host name sets per group.

        Args:
            data_all: Previously accumulated data, or None for the first batch.
            data_new: New batch of prepared data.

        Returns:
            Merged data with deduplicated host names per group.
        """
        if data_all is None:
            return data_new
        if data_new is None:
            return data_all

        merged_groups = {}

        for key, group in data_all.get('groups', {}).items():
            merged_groups[key] = {
                'organization_name': group['organization_name'],
                'collection_name': group['collection_name'],
                'host_names': set(group.get('host_names', [])),
            }

        for key, group in data_new.get('groups', {}).items():
            if key in merged_groups:
                merged_groups[key]['host_names'].update(group.get('host_names', []))
            else:
                merged_groups[key] = {
                    'organization_name': group['organization_name'],
                    'collection_name': group['collection_name'],
                    'host_names': set(group.get('host_names', [])),
                }

        all_host_names = set()
        for group in merged_groups.values():
            group['host_names'] = sorted(group['host_names'])
            group['host_count'] = len(group['host_names'])
            all_host_names.update(group['host_names'])

        agg_by_org = _aggregate_by_key(merged_groups, 'organization_name')
        agg_by_coll = _aggregate_by_key(merged_groups, 'collection_name')

        return {
            'groups': merged_groups,
            'by_organizations': [{'organization_name': k, 'host_count': v['host_count']} for k, v in agg_by_org.items()],
            'by_collections': [{'collection_name': k, 'host_count': v['host_count']} for k, v in agg_by_coll.items()],
            'indirect_nodes_total': len(all_host_names),
        }
