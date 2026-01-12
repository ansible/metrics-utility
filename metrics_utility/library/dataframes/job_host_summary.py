import pandas as pd

from metrics_utility.library.dataframes.base_traditional import (
    BaseTraditional,
    merge_arrays,
    merge_json_sets,
    merge_setdicts,
    merge_sets,
    parse_json_array,
)


DIRECT = 0
INDIRECT = 1
MANAGED_NODE_TYPES = {DIRECT: 'DIRECT', INDIRECT: 'INDIRECT'}


class DataframeJobHostSummary(BaseTraditional):
    TARBALL_NAMES = ['job_host_summary.csv', 'main_indirectmanagednodeaudit.csv', 'config.json']

    def prepare(self, tup):
        (jhs, mimna, config) = tup

        if jhs is not None:
            billing_data = jhs
            managed_node_type = DIRECT
        elif mimna is not None:
            billing_data = mimna
            managed_node_type = INDIRECT

        billing_data['managed_node_type'] = managed_node_type
        billing_data['managed_node_type_string'] = MANAGED_NODE_TYPES[managed_node_type]

        billing_data['organization_name'] = billing_data.organization_name.fillna('No organization name')
        billing_data['install_uuid'] = config['install_uuid']

        # Store the original host name for mapping purposes
        billing_data['original_host_name'] = billing_data['host_name']

        if 'ansible_host_variable' in billing_data.columns:
            # Replace missing ansible_host_variable with host name
            billing_data['ansible_host_variable'] = billing_data.ansible_host_variable.fillna(billing_data['host_name'])
            # And use the new ansible_host_variable instead of host_name, since
            # what is in ansible_host_variable should be the actual host we count
            billing_data['host_name'] = billing_data['ansible_host_variable']

        # Store ansible_host || hostname for tracking deduplication impact
        billing_data['host_names_before_dedup'] = billing_data['host_name']

        # Summarize all task counts into 1 col
        def sum_columns(row):
            return sum([row[i] for i in ['dark', 'failures', 'ok', 'skipped', 'ignored', 'rescued']])

        # Summarize all reachable task counts into 1 col
        def sum_reachable_columns(row):
            return sum([row[i] for i in ['failures', 'ok', 'skipped', 'ignored', 'rescued']])

        if managed_node_type == DIRECT:
            billing_data['task_runs'] = billing_data.apply(sum_columns, axis=1)

            # Filter out managed nodes that were unreachable (represented as the dark counter).
            # We want to count hosts that had at least one task running.
            billing_data['reachable_task_runs'] = billing_data.apply(sum_reachable_columns, axis=1)
            billing_data = billing_data[billing_data['reachable_task_runs'] > 0].copy()

            # Initialize with empty dicts - will be populated during deduplication if experimental dedup is enabled
            billing_data['facts'] = {}
            billing_data['canonical_facts'] = {}
            billing_data['events'] = None
        elif managed_node_type == INDIRECT:
            # Load the events array safely
            billing_data['events'] = billing_data['events'].apply(parse_json_array)

        billing_data['created'] = pd.to_datetime(billing_data['created'], format='ISO8601').dt.tz_localize(None)

        if 'job_created' in billing_data:
            billing_data['job_created'] = pd.to_datetime(billing_data['job_created'], format='ISO8601').dt.tz_localize(None)
        else:
            billing_data['job_created'] = pd.NaT

        return billing_data

    # Do the aggregation
    def group(self, dataframe):
        group = dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_name', 'count'),
            first_automation=('created', 'min'),
            last_automation=('created', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_type_string', set),
            # TODO: optimize the aggregation to keep less rows around
            # job_ids=('inventory_name', set),
            events=('events', merge_arrays),
            canonical_facts=('canonical_facts', merge_json_sets),
            facts=('facts', merge_json_sets),
            host_names_before_dedup=('host_names_before_dedup', set),
        )
        return self.cast_dataframe(group)

    # Merge pre-aggregated
    def regroup(self, dataframe):
        return dataframe.groupby(self.unique_index_columns(), dropna=False).agg(
            task_runs=('task_runs', 'sum'),
            host_runs=('host_runs', 'sum'),
            first_automation=('first_automation', 'min'),
            last_automation=('last_automation', 'max'),
            job_created=('job_created', 'max'),
            managed_node_type=('managed_node_type', 'min'),
            managed_node_types_set=('managed_node_types_set', merge_sets),
            events=('events', merge_arrays),
            canonical_facts=('canonical_facts', merge_setdicts),
            facts=('facts', merge_setdicts),
            host_names_before_dedup=('host_names_before_dedup', merge_sets),
        )

    @staticmethod
    def unique_index_columns():
        return ['organization_name', 'job_template_name', 'host_name', 'original_host_name', 'install_uuid', 'job_remote_id']

    @staticmethod
    def data_columns():
        return [
            'host_runs',
            'task_runs',
            'first_automation',
            'last_automation',
            'job_created',
            'managed_node_type',
            'managed_node_types_set',
            'canonical_facts',
            'facts',
            'events',
            'host_names_before_dedup',
        ]

    @staticmethod
    def cast_types():
        return {
            'task_runs': int,
            'host_runs': int,
            'managed_node_type': int,
            'first_automation': 'datetime64[ns]',
            'last_automation': 'datetime64[ns]',
            'job_created': 'datetime64[ns]',
        }

    @staticmethod
    def operations():
        return {
            'first_automation': 'min',
            'last_automation': 'max',
            'job_created': 'max',
            'managed_node_type': 'min',
            'managed_node_types_set': 'combine_set',
            'events': 'combine_set',
            'canonical_facts': 'combine_json_values',
            'facts': 'combine_json_values',
            'host_names_before_dedup': 'combine_set',
        }

    def dedup(self, dataframe, hostname_mapping=None, scope_dataframe=None, deduplicator=None):
        """
        Override dedup method to enrich canonical facts and facts from scope_dataframe
        when experimental deduplication is enabled.
        """
        if dataframe is None or dataframe.empty:
            return self.empty()

        if not hostname_mapping:
            return dataframe

        # Enrich direct managed nodes with canonical facts and facts from scope data when experimental deduplication is enabled
        # FIXME: dedup logic should NOT depend on deduplicator choice this way, just on is not None
        if deduplicator == 'ccsp-experimental' and scope_dataframe is not None and not scope_dataframe.empty:
            # Create a mapping from host_name to canonical_facts and facts
            if 'canonical_facts' in scope_dataframe.columns and 'facts' in scope_dataframe.columns:
                # Filter to only direct managed nodes for enrichment
                direct_mask = dataframe['managed_node_type'] == DIRECT  # DIRECT = 0

                if direct_mask.any():
                    host_facts_mapping = scope_dataframe.set_index('host_name')[['canonical_facts', 'facts']].to_dict('index')

                    # Enrich canonical_facts and facts for direct managed nodes
                    dataframe.loc[direct_mask, 'canonical_facts'] = dataframe.loc[direct_mask, 'host_name'].map(
                        lambda x: host_facts_mapping.get(x, {}).get('canonical_facts', {})
                    )
                    dataframe.loc[direct_mask, 'facts'] = dataframe.loc[direct_mask, 'host_name'].map(
                        lambda x: host_facts_mapping.get(x, {}).get('facts', {})
                    )

        # Call the parent dedup method to perform the actual deduplication
        return super().dedup(dataframe, hostname_mapping)
